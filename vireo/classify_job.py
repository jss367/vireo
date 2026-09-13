"""Classification job logic extracted from app.py.

This module contains the background work function for the /api/jobs/classify
endpoint. The route handler in app.py parses the request and delegates here.
"""

import contextlib
import json
import logging
import math
import os
import time
from dataclasses import dataclass

from labels import get_active_labels, get_saved_labels, load_merged_labels, read_label_file

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

from db import AUTO_MATCH_REVIEW_MARKER, Database, commit_with_retry
from keyword_normalization import folded_species_key as _folded_species_key
from keyword_normalization import normalize_keyword_display
from keyword_normalization import species_match_key as _species_match_key
from models import get_active_model, get_models
from resource_ledger import ResourceWaitCancelled

# ``_folded_species_key`` / ``_species_match_key`` used to be defined here.
# They moved to ``keyword_normalization`` when ``db`` needed the same fold
# for ``repair_mixed_species_prediction_groups`` — the repair's job is to
# reproduce the grouping decision ``_store_grouped_predictions`` makes
# below, so the two must not be able to drift. ``classify_job`` imports
# ``db``, so the shared home has to be a module both can import. The
# private aliases are kept because the grouping code and its tests read
# better with the leading underscore signalling "this module's rule".

try:
    from classifier import ClassificationCancelled, Classifier
except ImportError:
    Classifier = None

    class ClassificationCancelled(RuntimeError):
        pass

try:
    from timm_classifier import TimmClassifier
except ImportError:
    TimmClassifier = None

log = logging.getLogger(__name__)


@dataclass
class ClassifyParams:
    """Parameters for a classification job, parsed from the request body."""

    collection_id: str
    labels_file: str | None
    labels_files: list | None
    model_id: str | None
    model_name: str | None
    grouping_window: int
    similarity_threshold: float
    reclassify: bool


def _load_taxonomy(taxonomy_path):
    """Load taxonomy from JSON file. Returns Taxonomy instance or None."""
    if not os.path.exists(taxonomy_path):
        return None
    try:
        from taxonomy import Taxonomy

        return Taxonomy(taxonomy_path)
    except Exception as e:
        log.warning(
            "Could not load taxonomy: %s — continuing without taxonomy enrichment", e
        )
        return None


def _load_labels(
    model_type, model_str, labels_file, labels_files, db=None, model_dir=None,
):
    """Resolve labels for classification.

    ``model_dir`` — the on-disk directory of the active model. When given,
    the label-free ToL fallback checks that the model's ToL artifacts are
    actually installed (see `models.tree_of_life_ready`) before returning
    `use_tol=True`. Without this, a bioclip-2.5 install whose optional
    ToL files were skipped at download time would route to
    `Classifier(labels=None)` and crash with FileNotFoundError.

    Returns:
        (labels, use_tol, label_metas) — ``labels`` is a list of species
        strings or None; ``use_tol`` is True if Tree of Life mode should be
        used; ``label_metas`` is the list of label-set metadata dicts that
        actually produced ``labels`` (empty on the Tree of Life or timm
        paths). Downstream callers pass ``label_metas`` verbatim to
        ``describe_label_source`` and ``_record_labels_fingerprint`` so the
        displayed name and the fingerprint sources cannot disagree with the
        labels — even when the on-disk sources shift under our feet between
        this call and the display.
    """
    if model_type == "timm":
        log.info("Classification config: model=%s (timm) — no labels needed", model_str)
        return None, False, []

    labels = None
    label_metas: list[dict] = []

    # ``load_merged_labels`` silently skips paths whose file is missing on
    # disk (a saved list whose source was deleted, a workspace override still
    # pointing at a since-removed file). Retaining those metas here would let
    # ``describe_label_source`` name and count lists that contributed nothing,
    # and ``_record_labels_fingerprint`` would write the same false provenance
    # into ``labels_fingerprints``. Filter with the same predicate.
    def _existing_metas(metas):
        return [
            m for m in metas
            if m.get("labels_file") and os.path.exists(m["labels_file"])
        ]

    if labels_files and isinstance(labels_files, list):
        saved = get_saved_labels()
        saved_by_file = {s["labels_file"]: s for s in saved}
        requested = [
            saved_by_file.get(p, {"labels_file": p}) for p in labels_files
        ]
        label_metas = _existing_metas(requested)
        labels = load_merged_labels(label_metas)
        log.info("Using %d merged labels from %d sets", len(labels), len(label_metas))
    elif labels_file and os.path.exists(labels_file):
        labels = read_label_file(labels_file)
        if getattr(labels, "identities", {}):
            labels = load_merged_labels([{"labels_file": labels_file}])
        saved = get_saved_labels()
        saved_by_file = {s["labels_file"]: s for s in saved}
        label_metas = [saved_by_file.get(labels_file, {"labels_file": labels_file})]
        log.info("Using %d labels from file: %s", len(labels), labels_file)
    else:
        # Try workspace-scoped active labels first
        ws_labels = db.get_workspace_active_labels() if db else None
        if ws_labels is not None:
            saved = get_saved_labels()
            saved_by_file = {s["labels_file"]: s for s in saved}
            requested = [
                saved_by_file.get(p, {"labels_file": p}) for p in ws_labels
            ]
            label_metas = _existing_metas(requested)
            labels = load_merged_labels(label_metas)
            names = [s.get("name", "?") for s in label_metas]
            log.info(
                "Using %d merged labels from workspace active sets: %s",
                len(labels),
                ", ".join(names),
            )
        else:
            active_sets = get_active_labels()
            if active_sets:
                label_metas = _existing_metas(list(active_sets))
                labels = load_merged_labels(label_metas)
                names = [s.get("name", "?") for s in label_metas]
                log.info(
                    "Using %d merged labels from global active sets: %s",
                    len(labels),
                    ", ".join(names),
                )

    if labels:
        log.info(
            "Classification config: model=%s, labels=%d from %s",
            model_str,
            len(labels),
            labels_file or "active labels",
        )
    else:
        log.info("Classification config: model=%s, no labels selected", model_str)

    from models import supports_tree_of_life, tree_of_life_ready

    use_tol = False
    if not labels:
        label_metas = []
        if tree_of_life_ready(model_str, model_dir):
            log.info(
                "No regional labels available — using Tree of Life classifier (all species)"
            )
            use_tol = True
        elif supports_tree_of_life(model_str):
            # ToL-capable model but its optional artifacts weren't
            # installed on this host (e.g. bioclip-2.5 whose HF upload
            # of tol_embeddings.npy hasn't landed, or the optional
            # download was skipped). Surface the missing files
            # explicitly instead of letting Classifier(labels=None)
            # crash later with a FileNotFoundError.
            raise RuntimeError(
                f"No labels available and Tree of Life files "
                f"(tol_embeddings.npy, tol_classes.json) are not installed "
                f"for {model_str}. Go to Settings → Models and click "
                f"Repair, or Settings → Labels and download a species "
                f"list for your region."
            )
        else:
            raise RuntimeError(
                f"No labels available and Tree of Life mode is not supported "
                f"for {model_str}. Go to Settings > Labels and download "
                f"a species list for your region."
            )

    return labels, use_tol, label_metas


def _reuse_saved_label_embeddings(db, model_str, model_dir, labels, cancel_check=None):
    """Recover columns from older set caches using verified source lists.

    Older manifests omit the actual label order. Saved lists and run
    provenance are only candidates: their complete ordered cache key must
    match an existing payload before any column can be reused.
    """
    from classifier import _embedding_cache_service, _embedding_identity
    from embedding_cache import LabelEmbeddingCache, canonicalize_labels, identity_digest

    cache = _embedding_cache_service()
    if not os.path.isdir(cache.cache_dir):
        return
    candidates = {name for name in os.listdir(cache.cache_dir) if name.endswith(".npy")}
    if not candidates:
        return
    try:
        identity = _embedding_identity(labels, model_str, model_dir)
    except (OSError, ValueError):
        return  # normal model loading will report missing/invalid artifacts
    saved = get_saved_labels()
    by_file = {s["labels_file"]: s for s in saved if s.get("labels_file")}
    groups = [[path] for path in by_file]
    if db is not None:
        groups.extend(row["sources"] for row in db.get_labels_fingerprints() if row.get("sources"))
    checked = set()
    for sources in groups:
        if cancel_check and cancel_check():
            raise ClassificationCancelled("classification cancelled")
        try:
            sets = [by_file.get(path, {"labels_file": path}) for path in sources]
            variants = [load_merged_labels(sets)]
            if len(sources) == 1:
                variants.append(read_label_file(sources[0]))
            for variant in variants:
                candidate = {**identity, "labels": canonicalize_labels(variant)}
                digest = identity_digest(candidate)
                if digest in checked or f"{digest}.npy" not in candidates:
                    continue
                checked.add(digest)
                value = cache._load(digest, len(candidate["labels"]))
                LabelEmbeddingCache(cache.cache_dir, candidate, value.shape[0]).seed(candidate["labels"], value)
                log.info("Reused %d species labels from an existing set cache", len(candidate["labels"]))
        except (EOFError, OSError, ValueError):
            log.debug("Could not reuse a previous species label set", exc_info=True)


def _record_labels_fingerprint(
    db, fingerprint, labels, sources, full_fingerprint=None,
):
    """Populate the labels_fingerprints sidecar. Cosmetic — powers UX lookups."""
    display = ", ".join(os.path.basename(s) for s in (sources or [])) or None
    db.upsert_labels_fingerprint(
        fingerprint=fingerprint,
        display_name=display,
        sources=sources,
        label_count=len(labels or []),
        full_fingerprint=full_fingerprint,
    )


def _run_classifier_on_detection(db, detection_id, classifier_model, labels,
                                  labels_fingerprint, classify_fn=None):
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
        db.conn.execute(
            """INSERT OR REPLACE INTO predictions
                (detection_id, classifier_model, labels_fingerprint, species,
                 confidence, category, scientific_name,
                 taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
                 taxonomy_order, taxonomy_family, taxonomy_genus, source_taxon_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                detection_id,
                classifier_model,
                labels_fingerprint,
                species,
                confidence,
                pred.get("category", "new"),
                tax.get("scientific_name"),
                tax.get("kingdom"),
                tax.get("phylum"),
                tax.get("class"),
                tax.get("order"),
                tax.get("family"),
                tax.get("genus"),
                tax.get("taxon_id"),
            ),
        )
    commit_with_retry(db.conn)
    return predictions


def _classify_detection_gated(db, detection_id, classifier_model,
                               labels_fingerprint, labels, reclassify,
                               classify_fn=None):
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
        existing = db.get_classifier_run_keys(detection_id)
        if (classifier_model, labels_fingerprint) in existing:
            return []
    predictions = _run_classifier_on_detection(
        db, detection_id, classifier_model, labels,
        labels_fingerprint=labels_fingerprint,
        classify_fn=classify_fn,
    )
    if predictions:
        db.record_classifier_run(
            detection_id, classifier_model, labels_fingerprint,
            prediction_count=len(predictions),
        )
    return predictions


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

    filter_sql = ""
    filter_args = []
    if classifier_model is not None:
        filter_sql += " AND cr.classifier_model = ?"
        filter_args.append(classifier_model)
    if labels_fingerprint is not None:
        filter_sql += " AND cr.labels_fingerprint = ?"
        filter_args.append(labels_fingerprint)

    # Derive the set of expected classifier runtime_fingerprints.  A
    # classifier runtime encodes (model_identity, labels_full,
    # detector_runtime), so each distinct detector runtime in the
    # collection produces its own expected classifier runtime.  Empty
    # when the caller couldn't resolve either input (fresh install with
    # no downloaded weights) — the runtime filter is then omitted and
    # we keep the historic model/labels-only behavior.
    expected_runtimes = set()
    if model_identity is not None and labels_fingerprint_full:
        try:
            from computation_cache import classifier_runtime_fingerprint

            detector_runtimes = set()
            for chunk in _chunks(photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                for r in db.conn.execute(
                    f"""SELECT DISTINCT runtime_fingerprint
                         FROM detector_runs
                         WHERE photo_id IN ({placeholders})""",
                    list(chunk),
                ):
                    if r["runtime_fingerprint"]:
                        detector_runtimes.add(r["runtime_fingerprint"])
            for dr in detector_runtimes:
                expected = classifier_runtime_fingerprint(
                    model_identity, labels_fingerprint_full, dr,
                    taxonomy_identity=taxonomy_identity,
                )
                if expected:
                    expected_runtimes.add(expected)
        except (OSError, ValueError):
            expected_runtimes = set()

    runtime_clause = ""
    runtime_args = []
    if expected_runtimes:
        # Grandfather ``runtime_fingerprint = 'legacy'`` for pre-portable
        # rows, EXCEPT when the classifier is timm and its
        # ``label_descriptions.json`` enrichment has since transitioned
        # from missing to present. That mapping is what turns a raw
        # class id into the stored common name, so an unreviewed legacy
        # timm row's species is stale relative to what a run made today
        # would emit ("Bubulcus ibis" -> "Western Cattle-Egret"); keeping
        # the exception would leave the scientific names indefinitely
        # unless the user asked for a forced reclassify (Codex #1560 P2).
        # The manual-review branch below still authorizes such rows —
        # an explicit accept/reject is authoritative across enrichment
        # changes, matching the runtime-change pin exception.
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
        legacy_clause = (
            " OR cr.runtime_fingerprint = 'legacy'" if accept_legacy else ""
        )
        placeholders = ",".join("?" for _ in expected_runtimes)
        runtime_clause = (
            f" AND (cr.runtime_fingerprint IN ({placeholders})"
            f"{legacy_clause}"
            " OR EXISTS (SELECT 1 FROM predictions p"
            " JOIN prediction_review pr ON pr.prediction_id = p.id"
            " WHERE p.detection_id = cr.detection_id"
            " AND p.classifier_model = cr.classifier_model"
            " AND p.labels_fingerprint = cr.labels_fingerprint"
            " AND pr.status IN ('accepted', 'rejected')"
            " AND COALESCE(pr.individual, '') != ?))"
        )
        runtime_args = list(expected_runtimes) + [AUTO_MATCH_REVIEW_MARKER]

    # Only classifier_runs backed by at least one predictions row count as
    # cache-satisfying.  See docstring — a classifier_run without matching
    # predictions is a torn write from a crashed local job, not a
    # reusable cache row.
    predictions_exists = (
        "EXISTS (SELECT 1 FROM predictions pr "
        "WHERE pr.detection_id = cr.detection_id "
        "AND pr.classifier_model = cr.classifier_model "
        "AND pr.labels_fingerprint = cr.labels_fingerprint)"
    )
    classifiable_detection = (
        "(d.detector_model = 'full-image' OR "
        "(d.detector_model != 'full-image' "
        "AND COALESCE(d.category, 'animal') = 'animal' "
        "AND d.detector_confidence >= ?))"
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
        placeholders = ",".join("?" for _ in chunk)
        row = db.conn.execute(
            f"""SELECT COUNT(*) AS total,
                       SUM(CASE WHEN cr.detection_id IS NOT NULL
                                 AND {predictions_exists}
                                THEN 1 ELSE 0 END)
                         AS covered
                 FROM detections d
                 LEFT JOIN classifier_runs cr
                   ON cr.detection_id = d.id{filter_sql}{runtime_clause}
                WHERE {classifiable_detection}
                  AND d.photo_id IN ({placeholders})""",
            filter_args + runtime_args + [detector_confidence] + list(chunk),
        ).fetchone()
        total += (row["total"] if row else 0) or 0
        covered += (row["covered"] if row else 0) or 0

        photo_row = db.conn.execute(
            f"""SELECT COUNT(DISTINCT photo_id) AS covered_photos
                 FROM detections
                WHERE (detector_model = 'full-image' OR
                       (detector_model != 'full-image'
                        AND COALESCE(category, 'animal') = 'animal'
                        AND detector_confidence >= ?))
                  AND photo_id IN ({placeholders})""",
            [detector_confidence] + list(chunk),
        ).fetchone()
        covered_photos += (
            (photo_row["covered_photos"] if photo_row else 0) or 0
        )

        if identity_gate:
            # Track how many distinct (model, fp) pairs cover this
            # chunk's classifier_runs so the caller can't finalize
            # under a single identity when several are actually
            # present.  We short-circuit as soon as two are seen.
            # Same predictions-must-exist filter as the coverage
            # query: a torn classifier_runs row without predictions
            # is not a real identity that can satisfy the cache.
            for identity_row in db.conn.execute(
                f"""SELECT DISTINCT cr.classifier_model AS m,
                                    cr.labels_fingerprint AS fp
                     FROM detections d
                     JOIN classifier_runs cr
                       ON cr.detection_id = d.id
                    WHERE d.photo_id IN ({placeholders})
                      AND {classifiable_detection}
                      AND {predictions_exists}{filter_sql}{runtime_clause}""",
                list(chunk) + [detector_confidence] + filter_args + runtime_args,
            ):
                distinct_identities.add(
                    (identity_row["m"], identity_row["fp"])
                )
                if len(distinct_identities) > 1:
                    return False

    if total == 0 or covered != total:
        return False
    return covered_photos == len(photo_ids)


def _resolve_label_set_metas(params, db):
    """Return the label-set metadata dicts _load_labels would merge, in order.

    Mirrors the lookup order in _load_labels — explicit files first, then the
    workspace's active sets, then the global ones — but resolves only the
    sources, not the species names. Callers use it for the fingerprint row's
    source paths and for naming the sets in the UI.

    Prefer passing the ``label_metas`` returned by ``_load_labels`` directly
    to consumers so the two cannot disagree. This helper is the fallback for
    callers that have not yet plumbed those through, and it MUST reproduce
    ``_load_labels``'s fallback branches — otherwise a missing
    ``params.labels_file`` names the deleted path on the Jobs page and in
    ``labels_fingerprints`` while the actual classifier ran against the
    workspace or global lists.
    """
    saved_by_file = {}
    for meta in get_saved_labels() or []:
        path = meta.get("labels_file")
        if path:
            saved_by_file[path] = meta

    def _metas(paths):
        return [saved_by_file.get(p, {"labels_file": p}) for p in paths]

    # ``os.path.exists`` mirrors ``_load_labels``, which filters both the
    # plural and singular file branches through ``_existing_metas`` — a
    # configured path that has since been deleted contributes nothing to
    # ``labels`` there, so naming the stale path here would misreport the
    # label source. The plural branch keeps every path that still exists;
    # if all requested files are gone, the caller sees the same empty
    # metadata ``_load_labels`` would produce (Tree of Life falls in one
    # step below).
    if params.labels_files and isinstance(params.labels_files, list):
        return _metas([p for p in params.labels_files if os.path.exists(p)])
    if params.labels_file and os.path.exists(params.labels_file):
        return _metas([params.labels_file])
    ws_labels = db.get_workspace_active_labels() if db else None
    if ws_labels is not None:
        return _metas(list(ws_labels))
    return list(get_active_labels() or [])


def _sources_from_metas(label_metas):
    """Extract ``labels_file`` source paths from a list of metadata dicts."""
    return [
        meta.get("labels_file")
        for meta in (label_metas or [])
        if meta.get("labels_file")
    ]


def _resolve_label_sources(params, db):
    """Return list of source file paths used to build the active label set."""
    return _sources_from_metas(_resolve_label_set_metas(params, db))


def _label_set_name(meta):
    """Display name for one label set — its saved name, else its filename."""
    name = (meta.get("name") or "").strip()
    if name:
        return name
    return os.path.basename(meta.get("labels_file") or "") or "unnamed list"


def _describe_cached_label_source(
    db, photo_ids, *, classifier_model=None, labels_fingerprint=None,
    detector_confidence=0.0,
):
    """Name the label space of a reused-cache classify step when no peek loaded.

    Used only when ``_finalize_cached_only`` fires with ``peek_succeeded=False``
    — the label peek raised (missing files on a fresh install with an
    imported cache) so we have no live ``labels``/``use_tol``/``metas`` to
    hand ``describe_label_source``. The identity gate in
    ``_all_photos_cache_satisfied`` guarantees one canonical
    ``(classifier_model, labels_fingerprint)`` pair covers every reused row,
    so we can look up the corresponding ``labels_fingerprints`` sidecar and
    at least name the sources that originally produced these predictions.
    Returns ``None`` if we cannot recover any source — the caller then
    leaves the step unlabeled rather than inventing a description.

    ``detector_confidence`` — mirror the workspace threshold
    ``_all_photos_cache_satisfied`` used, so the fingerprint lookup only
    considers the same eligible detections. Without this, a photo also
    carrying runs on sub-threshold detections (or torn runs without any
    predictions row) could contribute a fingerprint that is not the one
    actually reused, so the persisted ``label_source`` would name a list
    that did not produce these predictions.
    """
    if not photo_ids:
        return None
    try:
        from db import _chunks

        found_fp = labels_fingerprint
        if found_fp is None:
            filter_sql = ""
            filter_args = []
            if classifier_model is not None:
                filter_sql = " AND cr.classifier_model = ?"
                filter_args.append(classifier_model)
            # Match ``_all_photos_cache_satisfied``'s eligibility filters:
            # only classifiable detections at the workspace threshold, and
            # only classifier_runs backed by an actual predictions row
            # (a torn run without predictions is not a real cache hit).
            classifiable_detection = (
                "(d.detector_model = 'full-image' OR "
                "(d.detector_model != 'full-image' "
                "AND COALESCE(d.category, 'animal') = 'animal' "
                "AND d.detector_confidence >= ?))"
            )
            predictions_exists = (
                "EXISTS (SELECT 1 FROM predictions p "
                "WHERE p.detection_id = cr.detection_id "
                "AND p.classifier_model = cr.classifier_model "
                "AND p.labels_fingerprint = cr.labels_fingerprint)"
            )
            for chunk in _chunks(photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                row = db.conn.execute(
                    f"""SELECT DISTINCT cr.labels_fingerprint AS fp
                         FROM detections d
                         JOIN classifier_runs cr
                           ON cr.detection_id = d.id{filter_sql}
                        WHERE d.photo_id IN ({placeholders})
                          AND {classifiable_detection}
                          AND {predictions_exists}
                          AND cr.labels_fingerprint IS NOT NULL
                        LIMIT 1""",
                    filter_args + list(chunk) + [detector_confidence],
                ).fetchone()
                if row and row["fp"]:
                    found_fp = row["fp"]
                    break
        if not found_fp:
            return "Reused cached predictions (label source unavailable)"

        sidecar = None
        for entry in db.get_labels_fingerprints() or []:
            if entry.get("fingerprint") == found_fp:
                sidecar = entry
                break
        if not sidecar:
            return "Reused cached predictions (label source unavailable)"

        sources = sidecar.get("sources") or []
        names = [os.path.basename(s) for s in sources if s]
        count = sidecar.get("label_count") or 0
        count_text = f"{count:,} species" if count else "cached label set"
        if not names:
            display = (sidecar.get("display_name") or "").strip()
            if display:
                return (
                    f"Reused cached predictions from "
                    f"{count_text}: {display}"
                )
            return "Reused cached predictions (label source unavailable)"
        if len(names) > 3:
            shown = ", ".join(names[:3]) + f" and {len(names) - 3} more"
        else:
            shown = ", ".join(names)
        if len(names) == 1:
            return f"Reused cached predictions from {count_text}: {shown}"
        return (
            f"Reused cached predictions from {count_text} across "
            f"{len(names)} lists: {shown}"
        )
    except Exception:
        log.debug(
            "Could not describe cached label source", exc_info=True,
        )
        return None


def describe_label_source(
    params, db, *, labels, use_tol, model_type, class_count=None,
    label_metas=None,
):
    """One line naming the label space a classify run compares photos against.

    The Jobs page names the model on each classify row, but the same model
    against a 1,300-species regional list and against the full Tree of Life
    are different classifiers as far as the results are concerned. The row has
    to say which list is in play, not just which weights are loaded.

    ``class_count`` is the constructed classifier's own label-space size, used
    for the two cases where the selected lists don't describe it: Tree of Life
    and a timm model's fixed head. Always returns a line — a run that reaches
    here has a label space, because _load_labels raises when it finds none.

    ``label_metas`` — pass the metadata list ``_load_labels`` returned to name
    the exact sets that produced ``labels``. Without it, this falls back to
    ``_resolve_label_set_metas`` which re-resolves from ``params`` and ``db``
    and can disagree with ``labels`` if the on-disk sources shifted (a
    configured file was deleted, or the workspace's active list was edited)
    between when the classifier loaded and when the row is rendered.
    """
    if model_type == "timm":
        if class_count:
            return (
                f"Model's own {class_count:,} built-in classes "
                "— species lists don't apply"
            )
        return "Model's own built-in classes — species lists don't apply"
    if use_tol or not labels:
        if class_count:
            return (
                f"Tree of Life: all {class_count:,} species "
                "(no species list active)"
            )
        return "Tree of Life: every species the model knows (no species list active)"

    if label_metas is None:
        metas = _resolve_label_set_metas(params, db)
    else:
        metas = label_metas
    names = [_label_set_name(m) for m in metas]
    count = f"{len(labels):,} species"
    if not names:
        return count
    if len(names) > 3:
        shown = ", ".join(names[:3]) + f" and {len(names) - 3} more"
    else:
        shown = ", ".join(names)
    if len(names) == 1:
        return f"{count} from {shown}"
    return f"{count} from {len(names)} lists: {shown}"


def _detect_batch(photos, folders, runner, job, reclassify, db,
                   det_conf_threshold=None, already_detected_ids=None,
                   cached_detections=None):
    """Run MegaDetector on a batch of photos.

    Same interface as _detect_subjects but designed to be called with
    partial photo lists for interleaved detect+classify in the streaming
    pipeline.  Does NOT push progress events — that is the caller's
    responsibility.

    Args:
        det_conf_threshold: Detection confidence threshold. If None,
            loaded from config (fallback for callers that don't pre-load).
        already_detected_ids: Set of photo IDs that already have detections
            in the database. Used for skip-if-already-detected logic.
        cached_detections: Optional dict {photo_id: [detection_dicts]}
            produced by a prior model in the same pipeline run. When
            provided and a photo is in already_detected_ids, the cached
            entries are used instead of db.get_detections() so that
            model 2+ binds to the exact detection rows from this run,
            not stale rows from a previous pipeline pass.

    Returns:
        (detection_map, detected_count, processed_ids) where detection_map
        is {photo_id: [list_of_detection_dicts]}, detected_count is total
        photos with at least one detection, and processed_ids is the set
        of photo IDs whose per-photo iteration completed without raising
        (callers use this to distinguish "ran and found nothing" from
        "never reached because an earlier photo raised mid-loop").
    """
    detected = 0
    detection_map = {}
    processed_ids: set[int] = set()
    if already_detected_ids is None:
        already_detected_ids = set()
    if cached_detections is None:
        cached_detections = {}

    try:
        if detect_animals is None or get_primary_detection is None:
            return detection_map, detected, processed_ids

        for photo in photos:
            folder_path = folders.get(photo["folder_id"], "")
            image_path = os.path.join(folder_path, photo["filename"])

            # Skip if already detected (unless reclassifying). After the
            # detector_runs migration, `already_detected_ids` includes
            # empty-scene photos (box_count=0) — we must not re-invoke
            # MegaDetector for them either. Either the detector produced
            # rows (reuse them) or it ran and found nothing (skip entirely).
            if not reclassify and photo["id"] in already_detected_ids:
                # Prefer cached detections from an earlier model in this
                # same pipeline run so that model 2+ is bound to the
                # detection rows just produced, not stale rows from a
                # prior pipeline pass that db.get_detections() would
                # return when old rows haven't been cleared.
                if cached_detections is not None and photo["id"] in cached_detections:
                    det_list = cached_detections[photo["id"]]
                    if det_list:
                        detection_map[photo["id"]] = det_list
                        detected += 1
                    processed_ids.add(photo["id"])
                    continue
                # Pull cached rows if any; an empty result means this photo
                # was scanned and had no animals, which is still a skip.
                # (Task 20 will add a min_conf filter to get_detections.)
                try:
                    existing_dets = db.get_detections(photo["id"])
                except Exception:
                    existing_dets = []
                if existing_dets:
                    det_list = []
                    for d in existing_dets:
                        det_list.append({
                            "id": d["id"],
                            "box_x": d["box_x"],
                            "box_y": d["box_y"],
                            "box_w": d["box_w"],
                            "box_h": d["box_h"],
                            "confidence": d["detector_confidence"],
                            "category": d["category"],
                            # sqlite3.Row supports [key] but not .get(); use try
                            # so test mocks (plain dicts without detector_model)
                            # don't crash this path.
                            # sqlite3.Row supports [key] but lacks .get(),
                            # and `key in row` is not supported either; .keys()
                            # is the documented contains-check. Fallback to None
                            # so test mocks (plain dicts without detector_model)
                            # don't crash this path.
                            "detector_model": (
                                d["detector_model"]
                                if "detector_model" in d.keys()  # noqa: SIM118
                                else None
                            ),
                        })
                    detection_map[photo["id"]] = det_list
                    detected += 1
                processed_ids.add(photo["id"])
                continue

            # Resolve workspace-effective threshold lazily on first actual
            # detection call so a batch where every photo hits the
            # cached/already-detected short-circuit doesn't need a working
            # config/db at all (the cached-detections short-circuit test
            # relies on this).
            #
            # The threshold is NOT passed to detect_animals — detector writes
            # everything above RAW_CONF_FLOOR so results can be globally
            # cached. The effective threshold is applied as a read-time
            # filter by get_detections / stats queries (Tasks 20-22).
            if det_conf_threshold is None:
                import config as cfg
                effective_cfg = db.get_effective_config(cfg.load())
                det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

            detections = detect_animals(image_path)

            if detections is None:
                # Detector run itself failed (image decode error, ONNX
                # error, etc.). Do NOT clear prior detections and do NOT
                # record a run — otherwise future non-reclassify passes
                # would skip the photo permanently, leaving it without
                # detections unless the user forces --reclassify.
                # The photo stays out of processed_ids so the caller
                # treats it as "will be retried next pass".
                continue

            # Persist detection rows and record the detector run atomically —
            # `write_detection_batch` wraps both writes in one transaction so a
            # crash between them can't leave a torn state (detections without a
            # matching detector_runs row, or the reverse for empty scenes).
            # Failures from `detect_animals` were handled by the ``is None``
            # early-continue above and must not poison the skip set.
            detector_runtime = job.get("_detector_runtime_fingerprint") or "legacy"
            portable_input = None
            portable_photo_hash = None
            if detector_runtime != "legacy":
                identity_row = db.conn.execute(
                    "SELECT file_hash, companion_path FROM photos WHERE id = ?",
                    (photo["id"],),
                ).fetchone()
                if (
                    identity_row is not None
                    and not identity_row["companion_path"]
                ):
                    try:
                        from computation_cache import source_input

                        portable_photo_hash = identity_row["file_hash"]
                        _input_block, portable_input = source_input(
                            portable_photo_hash, "vireo-detector-source-v1",
                        )
                    except (TypeError, ValueError):
                        portable_input = None
                        portable_photo_hash = None
            det_ids = db.write_detection_batch(
                photo["id"], "megadetector-v6", detections,
                runtime_fingerprint=detector_runtime,
                input_fingerprint=portable_input,
                force_runtime_replace=reclassify,
            )

            # Publication follows the database commit.  A store failure must
            # never turn successful inference into a failed detector run.
            #
            # ``write_detection_batch`` returns early (leaving the stored
            # detector_runs.runtime_fingerprint at its OLD value) when a
            # runtime change is proposed against a review-pinned run and
            # ``force_runtime_replace`` is False.  Publishing with the new
            # ``detector_runtime`` in that case would attach a foreign
            # runtime identity to boxes that were actually produced by an
            # older runtime — the artifact is content-addressed and
            # exportable, so downstream catalogs would trust that lie.
            # Read back the persisted runtime and only publish when it
            # equals the runtime we intended to write.
            if portable_input is not None:
                try:
                    from computation_cache import publish_detection_artifact

                    persisted_runtime_row = db.conn.execute(
                        """SELECT runtime_fingerprint
                           FROM detector_runs
                           WHERE photo_id = ?
                             AND detector_model = 'megadetector-v6'""",
                        (photo["id"],),
                    ).fetchone()
                    persisted_runtime = (
                        persisted_runtime_row["runtime_fingerprint"]
                        if persisted_runtime_row else None
                    )
                    if persisted_runtime == detector_runtime:
                        normalized_detections = [{
                            "box": {
                                "x": row["box_x"], "y": row["box_y"],
                                "w": row["box_w"], "h": row["box_h"],
                            },
                            "confidence": row["detector_confidence"],
                            "category": row["category"],
                        } for row in db.get_detections(
                            photo["id"], min_conf=0,
                            detector_model="megadetector-v6",
                        )]
                        # Reconstruct the configured ArtifactStore from
                        # the path stashed by ``run_classify_job`` /
                        # ``run_pipeline_job`` so newly published detector
                        # artifacts honor ``COMPUTATION_CACHE_DIR``
                        # instead of landing in the default location.
                        from computation_cache import ArtifactStore
                        _cache_dir = job.get("_computation_cache_dir")
                        _publish_store = (
                            ArtifactStore(_cache_dir) if _cache_dir else None
                        )
                        publish_detection_artifact(
                            portable_photo_hash,
                            "megadetector-v6",
                            detector_runtime,
                            normalized_detections,
                            store=_publish_store,
                        )
                except Exception:
                    log.warning(
                        "Could not publish portable detector result for photo %s",
                        photo["id"], exc_info=True,
                    )

            if detections:
                detected += 1

                # Build detection list with database IDs. Content-addressed IDs
                # can collapse near-duplicate detector outputs into one
                # persisted row, so fall back to the DB rows if the returned ID
                # count no longer matches the raw detector output count.
                det_list = []
                if len(det_ids) == len(detections):
                    for det, det_id in zip(detections, det_ids, strict=True):
                        det_list.append({
                            "id": det_id,
                            "box_x": det["box"]["x"],
                            "box_y": det["box"]["y"],
                            "box_w": det["box"]["w"],
                            "box_h": det["box"]["h"],
                            "confidence": det["confidence"],
                            "category": det.get("category", "animal"),
                            "detector_model": "megadetector-v6",
                        })
                else:
                    for det in db.get_detections(
                        photo["id"], min_conf=0,
                        detector_model="megadetector-v6",
                    ):
                        det_list.append({
                            "id": det["id"],
                            "box_x": det["box_x"],
                            "box_y": det["box_y"],
                            "box_w": det["box_w"],
                            "box_h": det["box_h"],
                            "confidence": det["detector_confidence"],
                            "category": det["category"],
                            "detector_model": det["detector_model"],
                        })
                detection_map[photo["id"]] = det_list

                # Mark as processed immediately after detection rows are committed
                # so that even if the quality-scoring calls below raise, the
                # reclassify purge in pipeline_job correctly removes the now-stale
                # pre-run detection rows for this photo rather than leaving them in
                # place and allowing future non-reclassify runs to reuse them.
                processed_ids.add(photo["id"])

            if detections:
                # Use highest-confidence detection as primary for quality scoring
                primary = get_primary_detection(detections)
                if primary and primary["confidence"] < det_conf_threshold:
                    # Top detection is below the workspace's detector_confidence
                    # threshold — it's noise, not a real subject. Skip scoring
                    # and clear any stale quality fields from a prior run so
                    # noise photos don't float to the top of highlights with a
                    # giant ``subject_size`` from a whole-frame noise box.
                    db.update_photo_quality(photo["id"])
                    primary = None
                if primary:
                    det_box = primary["box"]
                    subject_size = det_box["w"] * det_box["h"]

                    if compute_sharpness is not None:
                        overall_sharpness = compute_sharpness(image_path)
                        subject_sharpness = None
                        quality = 0

                        try:
                            from PIL import Image

                            img = Image.open(image_path)
                            try:
                                iw, ih = img.size
                                px = int(det_box["x"] * iw)
                                py = int(det_box["y"] * ih)
                                pw = int(det_box["w"] * iw)
                                ph = int(det_box["h"] * ih)
                                subject_sharpness = compute_sharpness(
                                    image_path, region=(px, py, pw, ph)
                                )
                            finally:
                                img.close()
                        except Exception:
                            subject_sharpness = overall_sharpness

                        if subject_sharpness is not None and subject_size is not None:
                            norm_sharp = min(1.0, math.log1p(subject_sharpness) / 10.0)
                            norm_size = min(1.0, subject_size * 4)
                            quality = round(0.7 * norm_sharp + 0.3 * norm_size, 4)

                        db.update_photo_quality(
                            photo["id"],
                            subject_sharpness=subject_sharpness,
                            subject_size=subject_size,
                            quality_score=quality,
                            sharpness=overall_sharpness,
                        )
                    else:
                        db.update_photo_quality(
                            photo["id"],
                        )

            processed_ids.add(photo["id"])

    except ResourceWaitCancelled:
        # Cooperative cancellation during a MegaDetector inference-lease
        # wait — the reclassify Stop path. Must propagate: the caller
        # already called ``clear_detections(photo["id"])`` for this
        # photo on the reclassify path (classify_job.py:1073), so if
        # this cancel were swallowed under the broad ``RuntimeError``
        # arm below, the classify recovery would then rebuild
        # predictions using the full-image fallback and land a
        # committed catalog change even though the user pressed Stop.
        # ``ResourceWaitCancelled`` subclasses ``RuntimeError``, so
        # this narrow arm MUST precede the broad one.
        raise
    except (ImportError, RuntimeError) as e:
        # Detection unavailable (missing weights/backend) — non-fatal, the
        # caller degrades to full-image classification. Previously silenced
        # entirely, which let the detect stage report success while the
        # batch's remaining photos were silently skipped.
        log.warning("Detection unavailable for batch (non-fatal): %s", e)
    except Exception:
        log.warning("Detection failed for batch (non-fatal)", exc_info=True)

    return detection_map, detected, processed_ids


def _detect_subjects(photos, folders, runner, job, reclassify, db):
    """Run MegaDetector on photos, storing quality metrics.

    Wraps _detect_batch with progress reporting for the standalone classify job.

    When ``reclassify`` is True, each photo's prior detections are cleared
    *just before* that photo is re-detected — not upfront for the whole
    scope. Cancelling mid-loop therefore leaves the unprocessed tail with
    its old state intact instead of an empty cache it can't rebuild. The
    cascaded predictions purge is handled by ``_classify_photos`` so that a
    mid-classify cancel (or a detection-setup failure that skips this loop
    entirely) doesn't strand photos with cleared predictions and no
    replacement.

    Returns:
        (detection_map, detected_count) where detection_map is
        {photo_id: [list_of_detection_dicts]} and detected_count is total
        photos with at least one detection.
    """
    total = len(photos)

    # Resolve cached-detection state before running MegaDetector so we can skip
    # the weight download entirely when every photo already has a detector_runs
    # row (including empty-scene rows with box_count=0).
    detector_runtime = None
    if not reclassify:
        try:
            from computation_cache import megadetector_runtime_fingerprint

            detector_runtime = megadetector_runtime_fingerprint()
        except (OSError, ValueError):
            detector_runtime = None
    already_detected_ids = (
        db.get_detector_run_photo_ids(
            "megadetector-v6", runtime_fingerprint=detector_runtime,
        )
        if not reclassify and detector_runtime is not None
        else db.get_detector_run_photo_ids("megadetector-v6")
        if not reclassify
        else set()
    )
    job["_detector_runtime_fingerprint"] = detector_runtime

    # Track photos whose state we mutated (clear_detections + write_detection_batch
    # in reclassify mode). Declared up here so the except handlers and early
    # returns can stash whatever was accumulated before the failure. The caller
    # reads ``job["_detect_processed_ids"]`` to decide which photos to classify
    # on a post-detect cancel — using ``detection_map.keys()`` alone would miss
    # empty-scene photos whose old predictions were already cascaded away.
    processed_for_rebuild: set[int] = set()

    try:
        if detect_animals is None or get_primary_detection is None:
            raise ImportError(
                "MegaDetector ONNX model not available — cannot run detection"
            )

        # Inside the try so a weights-download failure (e.g. network down)
        # degrades to full-image classification like every other detection
        # failure, instead of failing the whole job — which on a reclassify
        # run would strike after predictions/detections were already purged.
        # Require at least one photo — a no-op reclassify over 0 photos should
        # not trigger a ~300 MB MegaDetector download.
        needs_fresh_detection = bool(photos) and (
            reclassify or any(
                p["id"] not in already_detected_ids for p in photos
            )
        )
        if needs_fresh_detection:
            from detector import ensure_megadetector_weights

            def _dl_progress(phase, current, total_steps):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total_steps,
                        "current_file": "",
                        "phase": f"Step 4/5: {phase}",
                    },
                )

            # Gate the download. The classifier-init phase (run by the
            # caller before _detect_subjects) has no internal cancel check,
            # so a cancel during model load would otherwise land here only
            # to be ignored: hf_hub_download can't be interrupted once it
            # starts, so the per-photo cancel check below runs too late.
            if runner.is_cancelled(job["id"]):
                log.info(
                    "Classify job cancelled before MegaDetector weights download"
                )
                job["_detect_processed_ids"] = processed_for_rebuild
                return {}, 0

            weights_path = ensure_megadetector_weights(progress_callback=_dl_progress)
            from computation_cache import megadetector_runtime_fingerprint

            detector_runtime = megadetector_runtime_fingerprint(weights_path)
            job["_detector_runtime_fingerprint"] = detector_runtime
            if not reclassify:
                already_detected_ids = db.get_detector_run_photo_ids(
                    "megadetector-v6", runtime_fingerprint=detector_runtime,
                )

        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": "Loading MegaDetector...",
                "rate": 0,
                "phase": "Step 4/5: Detecting subjects",
            },
        )

        # Load config once for the entire detection loop. Use the
        # workspace-effective config so per-workspace overrides apply.
        import config as cfg
        effective_cfg = db.get_effective_config(cfg.load())
        det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

        # Process one photo at a time so we can report per-photo progress
        detection_map = {}
        detected = 0
        skipped_det = 0
        start_time = job.get("_start_time", time.time())

        for i, photo in enumerate(photos):
            if runner.is_cancelled(job["id"]):
                log.info(
                    "Classify job cancelled during detection (%d/%d)", i, total
                )
                break
            runner.update_step(
                job["id"], "detect",
                progress={"current": i + 1, "total": total},
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - start_time, 0.01), 1
                    ),
                    "phase": "Step 4/5: Detecting subjects",
                },
            )

            was_cached = (
                not reclassify
                and photo["id"] in already_detected_ids
            )

            # Per-photo reclassify purge: wipe this photo's prior
            # detections immediately before re-detecting. The old
            # global-upfront purge wiped the entire scope before detection
            # started, so a mid-detection cancel stranded the unprocessed
            # tail with no predictions and no detections. Doing it per-photo
            # means an early cancel preserves the untouched photos' cached
            # state.
            #
            # The cascaded ``predictions`` clear is deferred to the
            # classification loop's own per-photo purge: a mid-classify
            # cancel would otherwise leave the unclassified tail with
            # fresh detections but zero predictions, and a detection-setup
            # failure (missing weights, etc.) that skips this loop entirely
            # would leave stale predictions alongside the fallback
            # full-image classifier's output. Tying the predictions clear
            # to the classify loop instead means both cases preserve or
            # rebuild the predictions in lockstep with the new run.
            if reclassify:
                db.clear_detections(photo["id"])
                # Once the clear has run we own rebuilding this photo on
                # cancel — including the cases where ``_detect_batch``
                # doesn't add the id to ``batch_processed`` (e.g.
                # ``detect_animals`` returns None for a decode failure,
                # or the batch swallows an exception). Without this the
                # photo would be left with cleared detections/predictions
                # and no replacement. The full-image fallback in
                # ``_classify_photos`` handles photos with no detection
                # rows, so registering here unconditionally is safe.
                processed_for_rebuild.add(photo["id"])

            batch_map, batch_detected, _batch_processed = _detect_batch(
                [photo], folders, runner, job, reclassify, db,
                det_conf_threshold=det_conf_threshold,
                already_detected_ids=already_detected_ids,
            )
            detection_map.update(batch_map)
            detected += batch_detected

            if was_cached and batch_detected:
                skipped_det += 1

        log.info(
            "Detection done: %d animals detected out of %d photos (%d skipped, already detected)",
            detected,
            total,
            skipped_det,
        )
    except (ImportError, RuntimeError) as e:
        msg = str(e)
        if "ONNX model not available" in msg or "not found" in msg:
            log.warning(
                "MegaDetector weights not available — detection skipped; classifying full images. "
                "Download the MegaDetector V6 ONNX model from the pipeline models page to enable "
                "subject detection, cropped classification, and mask extraction."
            )
            job["errors"].append(
                "MegaDetector weights not downloaded — detection skipped. Classification ran on full "
                "images (less accurate) and no detections were stored, which also prevents the mask "
                "extraction stage from producing subject masks. Download MegaDetector V6 from the "
                "pipeline models page to fix."
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": "Step 4/5: Detection skipped — MegaDetector weights not downloaded",
                },
            )
        else:
            log.warning("Detection unavailable: %s — classifying full images", e)
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": "",
                    "phase": f"Step 4/5: Detection failed — {msg[:120]}",
                },
            )
            job["errors"].append(f"Detection unavailable: {msg[:200]}")
        detection_map = {}
        detected = 0
    except Exception as e:
        log.warning(
            "Detection failed (non-fatal) — classifying full images", exc_info=True
        )
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": "",
                "phase": f"Step 4/5: Detection failed — {str(e)[:120]}",
            },
        )
        job["errors"].append(f"Detection failed: {str(e)[:200]}")
        detection_map = {}
        detected = 0

    job["_detect_processed_ids"] = processed_for_rebuild
    return detection_map, detected


_BATCH_SIZE = 16


def _prepare_image(photo, folders, detection, vireo_dir=None):
    """Load and crop a photo to a specific detection's bounding box.

    Args:
        photo: photo dict
        folders: {folder_id: path} mapping
        detection: detection dict with box_x, box_y, box_w, box_h keys
            (or None for full image classification)
        vireo_dir: optional path to ~/.vireo/; when set, tries to load the
            pre-extracted working copy JPEG before falling back to the
            original file via load_image().

    Returns:
        (PIL.Image, folder_path, image_path) or (None, folder_path, image_path) on failure.
    """
    from PIL import Image

    folder_path = folders.get(photo["folder_id"], "")
    image_path = os.path.join(folder_path, photo["filename"])

    img = None
    input_source = "original"
    if vireo_dir and load_working_image is not None:
        img, input_source = load_working_image(
            photo, vireo_dir, max_size=None, folders=folders,
            return_source=True,
        )
    if img is None:
        img = load_image(image_path, max_size=None)
        input_source = "original"
    if img is None:
        return None, folder_path, image_path

    # Crop to detection bounding box with padding
    if detection:
        iw, ih = img.size
        pad_w = detection["box_w"] * 0.2
        pad_h = detection["box_h"] * 0.2
        x1 = max(0, int((detection["box_x"] - pad_w) * iw))
        y1 = max(0, int((detection["box_y"] - pad_h) * ih))
        x2 = min(iw, int((detection["box_x"] + detection["box_w"] + pad_w) * iw))
        y2 = min(ih, int((detection["box_y"] + detection["box_h"] + pad_h) * ih))
        crop = img.crop((x1, y1, x2, y2))
        if crop.size[0] >= 50 and crop.size[1] >= 50:
            img.close()
            img = crop
        else:
            crop.close()

    img.thumbnail((1024, 1024), Image.LANCZOS)
    # Classification artifacts are published after predictions are persisted,
    # which may be long after this image was loaded. Carry the actual pixel
    # source with the in-memory image so quota eviction cannot make a
    # working-copy-backed result look original-backed by clearing the row.
    img.info["_vireo_input_source"] = input_source
    return img, folder_path, image_path


def _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=1):
    """Classify a batch of prepared images and append results.

    Returns the number of failures within this batch.
    """
    from datetime import datetime as dt

    images = [entry["img"] for entry in batch]
    failed = 0

    try:
        # GPU serialisation across concurrent pipelines lives inside the
        # classifier helpers (around the ``session.run`` calls), so this
        # path holds no process-wide lock around preprocessing, DB upserts,
        # or result-building. See ``Classifier._get_image_embedding`` and
        # ``TimmClassifier.classify[_batch]``.
        try:
            if model_type == "timm":
                batch_preds = clf.classify_batch(images, threshold=0)
                batch_results = [(preds, None) for preds in batch_preds]
            else:
                batch_results = clf.classify_batch_with_embedding(images, threshold=0)
        except Exception:
            log.warning("Batch classification failed, falling back to single-image", exc_info=True)
            batch_results = []
            for entry in batch:
                try:
                    if model_type == "timm":
                        preds = clf.classify(entry["img"], threshold=0)
                        batch_results.append((preds, None))
                    else:
                        preds, emb = clf.classify_with_embedding(entry["img"], threshold=0)
                        batch_results.append((preds, emb))
                except Exception:
                    log.warning("Classification failed for %s", entry["photo"]["filename"], exc_info=True)
                    batch_results.append(None)
                    failed += 1

        for entry, result in zip(batch, batch_results, strict=True):
            if result is None:
                continue
            all_preds, embedding = result

            if embedding is not None:
                emb_bytes = embedding.tobytes()
                db.upsert_photo_embedding(
                    entry["photo"]["id"], model_name, emb_bytes,
                )
                # Also key by detection so multi-subject reruns from
                # cache pick each detection's own vector instead of the
                # last-detection-wins photo-level row. Without this, two
                # subjects on the same photo would share one cached
                # embedding and refine_groups_by_similarity would merge
                # them (or misgroup them across a burst) on non-reclassify
                # reruns even though no fresh inference ran.
                det_id = entry.get("detection_id")
                if det_id is not None:
                    db.upsert_photo_embedding(
                        entry["photo"]["id"], model_name, emb_bytes,
                        variant=f"det:{det_id}",
                    )

            if not all_preds:
                continue

            top = all_preds[0]
            log.info(
                '%s: "%s" at %.0f%%',
                entry["photo"]["filename"],
                top["species"],
                top["score"] * 100,
            )

            timestamp = None
            if entry["photo"]["timestamp"]:
                try:
                    timestamp = dt.fromisoformat(entry["photo"]["timestamp"])
                except Exception:
                    pass

            # Build alternatives list (predictions 2..top_k)
            alternatives = []
            for alt_pred in all_preds[1:top_k]:
                alternatives.append({
                    "species": alt_pred["species"],
                    "confidence": alt_pred["score"],
                    "taxonomy": alt_pred.get("taxonomy"),
                })

            raw_result = {
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": top["species"],
                "confidence": top["score"],
                "timestamp": timestamp,
                "filename": entry["photo"]["filename"],
                "embedding": embedding,
                "taxonomy": top.get("taxonomy"),
                "alternatives": alternatives,
            }
            input_source = getattr(entry["img"], "info", {}).get(
                "_vireo_input_source"
            )
            if input_source in ("original", "working_copy"):
                raw_result["_input_source"] = input_source
            raw_results.append(raw_result)
    finally:
        # Close all PIL images to avoid resource leaks
        for entry in batch:
            entry["img"].close()

    return failed


def _classify_photos(
    photos, folders, detection_map, existing_preds, clf, model_type,
    model_name, runner, job, db, top_k=1, vireo_dir=None,
    labels_fingerprint=None, reclassify=False,
    finish_cleared_only=False,
):
    """Classify detections in batches, cropping to each detection's bounding box.

    For each photo, iterates over all detections (from detection_map) and
    classifies each one independently. Photos without detections are
    classified as full images.

    Images are passed directly to classifiers as PIL objects (no temp file I/O).
    Multiple images are batched into a single forward pass for throughput.

    A per-detection classifier_runs gate keyed on (detection_id, model_name,
    labels_fingerprint) short-circuits re-work when the same triple already
    ran. reclassify=True bypasses the gate.

    ``finish_cleared_only`` is set by ``run_classify_job`` when a post-detect
    cancel landed in reclassify mode. The processed subset of ``photos``
    already had its prior detections + cascaded predictions wiped during
    detection; we must run classification on them anyway to avoid stranding
    them with no predictions. In this mode the per-photo predictions clear
    is skipped (the cascade in ``_detect_subjects`` already did it) and the
    cancel-break at the top of the loop is suppressed (we're rebuilding the
    already-cleared work, not starting new work).

    Returns:
        (raw_results, failed_count, skipped_existing_count)
    """
    # Fall back to the legacy sentinel when the caller didn't compute a
    # fingerprint — matches the default used by classifier_runs.
    fp = labels_fingerprint or "legacy"
    from datetime import datetime as dt

    if load_image is None:
        raise ImportError("image_loader module is required for classification")

    raw_results = []
    failed = 0
    skipped_existing = 0
    total = len(photos)
    batch = []
    cancelled = False
    portable_labels_full = job.get("_labels_fingerprint_full")
    portable_model_identity = job.get("_classifier_model_identity")
    portable_taxonomy_identity = job.get("_taxonomy_identity", "no-tax")

    # On a reclassify run every photo has its predictions cleared BEFORE
    # being queued into ``batch`` (per-photo ``clear_predictions`` below
    # at line 1442). Any subsequent ``_flush_batch`` failure therefore
    # strands those photos empty — including the mid-loop 16-image
    # flushes at lines 1568 / 1696, not just the tail flush at line
    # 1735. Suspend the bound resource cancel probe around EVERY flush
    # on a reclassify so ``acquire_inference_resources`` inside
    # ``_flush_batch`` completes even if cancel arrived while the flush
    # was in flight. The loop-level cancel check at line 1399 uses
    # ``runner.is_cancelled`` directly (not the bound probe), so the
    # loop still breaks promptly on cancel; only in-progress and tail
    # flushes are protected. Non-reclassify flushes stay unaffected.
    from resource_ledger import bind_resource_cancel_check

    def _flush_preserving_cleared():
        if reclassify:
            return bind_resource_cancel_check(None)
        return contextlib.nullcontext()

    def _runtime_aware_run_keys(detection_id):
        expected_runtime = None
        if portable_labels_full and portable_model_identity:
            try:
                from computation_cache import classifier_runtime_for_detection

                expected_runtime = classifier_runtime_for_detection(
                    db,
                    detection_id,
                    portable_model_identity,
                    portable_labels_full,
                    taxonomy_identity=portable_taxonomy_identity,
                )
            except (OSError, ValueError):
                expected_runtime = None
        if expected_runtime is None:
            return db.get_classifier_run_keys(detection_id)
        return db.get_classifier_run_keys(
            detection_id, runtime_fingerprint=expected_runtime,
        )

    start_time = time.time()

    for i, photo in enumerate(photos):
        if not finish_cleared_only and runner.is_cancelled(job["id"]):
            # Already-classified work is in raw_results (committed via the
            # in-loop flushes); the pending `batch` is queued items that
            # haven't run through the model yet. In reclassify mode the
            # final flush still drains the batch (those photos already had
            # their old predictions cleared above and would otherwise end
            # up empty); in non-reclassify mode the batch is dropped to
            # honor the cancel signal. The per-photo reclassify clear
            # below only fires for photos we actually reach, so the
            # unclassified tail keeps its old predictions intact.
            log.info(
                "Classify job cancelled during classification (%d/%d)", i, total
            )
            cancelled = True
            break

        # Per-photo reclassify predictions purge. Lives here (rather than
        # alongside ``clear_detections`` in the detection loop) so that:
        #   1. A mid-classify cancel leaves the unprocessed tail with its
        #      old predictions intact — without this gate they'd already
        #      be cleared and the cancel would strand them with new
        #      detections and no predictions.
        #   2. When detection setup fails (missing weights, etc.) and the
        #      job degrades to full-image classification, the stale
        #      detector-based predictions still get replaced rather than
        #      lingering alongside the fallback model's output.
        # ``clear_predictions`` also wipes the matching ``classifier_runs``
        # rows so the per-detection skip gate doesn't short-circuit the
        # fresh inference about to run. Skipped in ``finish_cleared_only``
        # mode because the cascade in ``_detect_subjects`` already wiped
        # the prior predictions for these photos.
        #
        # No ``labels_fingerprint`` filter: in the normal reclassify path
        # ``clear_detections`` already cascade-wiped this photo's
        # predictions (across all fingerprints) before we got here, so a
        # filtered clear would just be a no-op repeat. In the fallback
        # path (detection setup failed → empty ``detection_map`` for this
        # photo, old detector detections still on disk) this clear is the
        # ONLY purge before the full-image fallback writes new predictions;
        # scoping it to the current fingerprint would leave predictions
        # under prior fingerprints intact (e.g. after a workspace
        # label-set change), and ``get_predictions``' latest-fingerprint
        # filter would then surface them alongside the new fallback rows.
        if reclassify and not finish_cleared_only:
            db.clear_predictions(
                model=model_name,
                collection_photo_ids=[photo["id"]],
            )

        job["progress"]["current"] = i + 1
        job["progress"]["current_file"] = photo["filename"]
        runner.update_step(
            job["id"], "classify",
            progress={"current": i + 1, "total": total},
        )
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": i + 1,
                "total": total,
                "current_file": photo["filename"],
                "rate": round((i + 1) / max(time.time() - start_time, 0.01), 1),
                "phase": "Step 5/5: Classifying species",
            },
        )

        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])

        # Get detections for this photo (list of detection dicts with IDs)
        photo_detections = detection_map.get(photo["id"], [])

        if photo_detections:
            # Classify each detection independently.
            #
            # No photo-level short-circuit here: a prior short-circuit that
            # skipped photos with any cached prediction under (model, fp)
            # silently dropped newly-surfaced detections after the user
            # lowered `detector_confidence`, leaving them unclassified
            # until --reclassify. The per-detection classifier_runs gate
            # below handles incremental work correctly.
            timestamp = None
            if photo["timestamp"]:
                try:
                    timestamp = dt.fromisoformat(photo["timestamp"])
                except Exception:
                    pass

            for detection in photo_detections:
                # Classifier-run gate: if (detection, model, fingerprint)
                # has a run key AND has cached prediction rows, surface the
                # cached top-1 and skip inference. If the run key exists
                # but no cached rows do (e.g. the prior pass stored
                # `category == 'match'` which is intentionally not written,
                # or transient ordering between record_classifier_run and
                # _store_grouped_predictions), DON'T short-circuit —
                # otherwise the photo is stranded until the user forces
                # --reclassify. Fall through to re-classify instead.
                if not reclassify:
                    run_keys = _runtime_aware_run_keys(detection["id"])
                    if (model_name, fp) in run_keys:
                        cached = db.get_predictions_for_detection(
                            detection["id"],
                            classifier_model=model_name,
                            labels_fingerprint=fp,
                            min_classifier_conf=0,
                        )
                        if cached:
                            skipped_existing += 1
                            top = cached[0]  # ordered by confidence DESC
                            embedding = None
                            if model_type != "timm":
                                # Prefer per-detection variant so
                                # multi-subject photos don't reuse the
                                # last-detection-wins photo-level vector
                                # (see _flush_batch). Fall back to the
                                # photo-level entry only when this photo
                                # has a single qualifying detection so
                                # legacy data still refines correctly.
                                emb_blob = db.get_photo_embedding(
                                    photo["id"], model_name,
                                    variant=f"det:{detection['id']}",
                                )
                                if (
                                    not emb_blob
                                    and len(photo_detections) == 1
                                ):
                                    emb_blob = db.get_photo_embedding(
                                        photo["id"], model_name,
                                    )
                                if emb_blob:
                                    import numpy as np
                                    embedding = np.frombuffer(
                                        emb_blob, dtype=np.float32,
                                    )
                            raw_results.append({
                                "photo": photo,
                                "detection_id": detection["id"],
                                "folder_path": folder_path,
                                "image_path": image_path,
                                "prediction": top["species"],
                                "confidence": top["confidence"],
                                "timestamp": timestamp,
                                "filename": photo["filename"],
                                "embedding": embedding,
                                "taxonomy": _cached_prediction_taxonomy(top),
                                "alternatives": [],
                                "_existing": True,
                            })
                            continue
                        # Run key without cached rows → fall through to
                        # classify this detection.

                img, det_folder_path, det_image_path = _prepare_image(
                    photo, folders, detection, vireo_dir=vireo_dir
                )
                if img is None:
                    failed += 1
                    continue

                batch.append({
                    "photo": photo,
                    "detection_id": detection["id"],
                    "folder_path": det_folder_path,
                    "image_path": det_image_path,
                    "img": img,
                })

                if len(batch) >= _BATCH_SIZE:
                    pre_len = len(raw_results)
                    with _flush_preserving_cleared():
                        failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
                    _record_batch_classifier_runs(
                        db, batch, model_name, fp, raw_results, pre_len,
                        labels_fingerprint_full=job.get("_labels_fingerprint_full"),
                        model_identity=job.get("_classifier_model_identity"),
                    )
                    batch = []
        else:
            # No detections — use (or create) a full-image synthetic detection
            # to carry the classifier output. save_detections is now idempotent
            # under content-addressed IDs, but reading the existing row first
            # avoids an UPSERT + stale-cleanup roundtrip on the common path.
            # min_conf=0 because the synthetic full-image detection is
            # written with confidence=0 — the default threshold filter would
            # hide it.
            existing_full = db.get_detections(
                photo["id"], detector_model="full-image", min_conf=0,
            )
            if existing_full and not reclassify:
                full_det_id = existing_full[0]["id"]
            else:
                full_image_det = [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                   "confidence": 0, "category": "animal"}]
                from computation_cache import (
                    full_image_runtime_fingerprint,
                    source_input,
                )
                full_runtime = full_image_runtime_fingerprint()
                identity = db.conn.execute(
                    "SELECT file_hash, companion_path FROM photos WHERE id = ?",
                    (photo["id"],),
                ).fetchone()
                full_input = None
                if identity is not None and not identity["companion_path"]:
                    try:
                        _block, full_input = source_input(
                            identity["file_hash"], "vireo-detector-source-v1",
                        )
                    except ValueError:
                        full_input = None
                # Wrap both writes in the single ``write_detection_batch``
                # transaction so a crash between save_detections and
                # record_detector_run can't leave a full-image detection
                # row without its matching detector_runs row — that torn
                # state would fool the runtime-aware reuse gates into
                # treating the photo as never detected.
                full_det_ids = db.write_detection_batch(
                    photo["id"], "full-image", full_image_det,
                    runtime_fingerprint=full_runtime,
                    input_fingerprint=full_input,
                )
                full_det_id = full_det_ids[0]
            # Gate check for the synthetic full-image detection too.
            # Mirror the regular detection branch: when gated, surface the
            # cached top-1 prediction into raw_results so downstream
            # grouping/storage still sees it. Without this, non-reclassify
            # reruns silently drop cached full-image photos even though
            # those photos were intentionally kept in the cache.
            if not reclassify:
                run_keys = _runtime_aware_run_keys(full_det_id)
                if (model_name, fp) in run_keys:
                    cached = db.get_predictions_for_detection(
                        full_det_id,
                        classifier_model=model_name,
                        labels_fingerprint=fp,
                        min_classifier_conf=0,
                    )
                    if cached:
                        skipped_existing += 1
                        top = cached[0]
                        timestamp = None
                        if photo["timestamp"]:
                            try:
                                timestamp = dt.fromisoformat(photo["timestamp"])
                            except Exception:
                                pass
                        embedding = None
                        if model_type != "timm":
                            # Full-image detections are always single per
                            # (photo, model), so the photo-level row is
                            # unambiguously theirs. Prefer the per-detection
                            # variant for parity with new writes; fall back
                            # to the photo-level entry.
                            emb_blob = db.get_photo_embedding(
                                photo["id"], model_name,
                                variant=f"det:{full_det_id}",
                            )
                            if not emb_blob:
                                emb_blob = db.get_photo_embedding(
                                    photo["id"], model_name,
                                )
                            if emb_blob:
                                import numpy as np
                                embedding = np.frombuffer(
                                    emb_blob, dtype=np.float32,
                                )
                        raw_results.append({
                            "photo": photo,
                            "detection_id": full_det_id,
                            "folder_path": folder_path,
                            "image_path": image_path,
                            "prediction": top["species"],
                            "confidence": top["confidence"],
                            "timestamp": timestamp,
                            "filename": photo["filename"],
                            "embedding": embedding,
                            "taxonomy": _cached_prediction_taxonomy(top),
                            "alternatives": [],
                            "_existing": True,
                        })
                        continue
                    # Run key without cached rows → fall through to
                    # re-classify this full-image detection.
            img, folder_path, image_path = _prepare_image(photo, folders, None, vireo_dir=vireo_dir)
            if img is None:
                failed += 1
                continue

            batch.append({
                "photo": photo,
                "detection_id": full_det_id,
                "folder_path": folder_path,
                "image_path": image_path,
                "img": img,
            })

            if len(batch) >= _BATCH_SIZE:
                pre_len = len(raw_results)
                with _flush_preserving_cleared():
                    failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
                _record_batch_classifier_runs(
                    db, batch, model_name, fp, raw_results, pre_len,
                    labels_fingerprint_full=job.get("_labels_fingerprint_full"),
                    model_identity=job.get("_classifier_model_identity"),
                )
                batch = []

    # Flush remaining images. The pending batch holds photos that haven't
    # been classified yet — for non-reclassify cancels we drop it (those
    # photos still have their cached predictions, so honoring the cancel
    # signal here just skips wasted inference). For reclassify cancels we
    # must flush instead: each queued photo already had its old predictions
    # wiped by the per-photo ``clear_predictions`` above, and bailing here
    # would strand them with no predictions until a manual rerun. Flushing
    # finishes the rebuild for the queued tail without picking up any new
    # photos (the cancel check at the top of the loop still blocks those).
    if batch and (not cancelled or reclassify):
        # A reclassify tail flush is a deliberate preservation pass.
        # CPU inference consults the bound resource cancel probe, which
        # is already True on this cancelled path, so leaving the
        # binding active would make ``_flush_batch`` raise
        # ``ResourceWaitCancelled`` for the whole batch, the fallback
        # per-image path would also raise, and no replacement
        # predictions would be written for the queued photos whose old
        # predictions the loop already cleared. The
        # ``_flush_preserving_cleared`` helper suspends the binding on
        # every reclassify flush so inference completes; ``JobRunner``
        # still owns any hard shutdown via the runner-side deadline.
        pre_len = len(raw_results)
        with _flush_preserving_cleared():
            failed += _flush_batch(batch, clf, model_type, model_name, db, raw_results, top_k=top_k)
        _record_batch_classifier_runs(
            db, batch, model_name, fp, raw_results, pre_len,
            labels_fingerprint_full=job.get("_labels_fingerprint_full"),
            model_identity=job.get("_classifier_model_identity"),
        )

    return raw_results, failed, skipped_existing


def _record_batch_classifier_runs(
    db, batch, model_name, labels_fingerprint, raw_results, raw_results_start=0,
    labels_fingerprint_full=None, model_identity=None,
):
    """Record classifier_runs rows for every detection in ``batch``.

    ``batch`` is the list of entries that were just passed to _flush_batch.
    ``raw_results_start`` is ``len(raw_results)`` captured *before* that
    _flush_batch call, so only the rows this flush appended
    (``raw_results[raw_results_start:]``) are tallied. Earlier rows belong to
    prior batches; re-scanning the whole growing list on every flush would
    make the bookkeeping O(n^2) across a large collection. Called after
    _flush_batch has appended to ``raw_results`` so the run row is only
    written for detections that actually produced classifier output.

    Portable-artifact promotion (``promote_and_publish_classifier_run``) is
    NOT done here even when ``labels_fingerprint_full`` /
    ``model_identity`` are provided — that function reads the persisted
    ``predictions`` rows to synthesize the exportable artifact, and those
    rows are not written until ``_store_grouped_predictions`` runs later.
    Callers must invoke ``_publish_classifier_runs_for_raw_results`` after
    ``_store_grouped_predictions`` completes so fresh runs do not stay
    stranded on ``runtime_fingerprint = 'legacy'``.
    """
    if not batch:
        return
    # Tally how many of this flush's raw_results entries reference each
    # detection_id. Entries without a detection_id (unusual, but possible on
    # synthesized rows) are ignored. For a per-detection batch this is
    # typically 0 or 1.
    counts: dict = {}
    for r in raw_results[raw_results_start:]:
        did = r.get("detection_id")
        if did is not None:
            counts[did] = counts.get(did, 0) + 1
    seen: set = set()
    for entry in batch:
        did = entry.get("detection_id")
        if did is None or did in seen:
            continue
        seen.add(did)
        # Only record the run for detections that actually produced a
        # prediction. A count of 0 means the classifier failed (transient
        # load error, decode error, etc.) — caching it as "done" would
        # permanently strand the detection on the next non-reclassify run.
        n = counts.get(did, 0)
        if n <= 0:
            continue
        db.record_classifier_run(
            did, model_name, labels_fingerprint,
            prediction_count=n,
            labels_fingerprint_full=labels_fingerprint_full,
        )


def _publish_classifier_runs_for_raw_results(
    db, raw_results, classifier_model, labels_fingerprint,
    labels_fingerprint_full=None, model_identity=None,
    taxonomy_identity="no-tax", store=None,
):
    """Publish portable classifier artifacts for freshly-classified detections.

    ``promote_and_publish_classifier_run`` reads the persisted ``predictions``
    rows for a detection to build the exportable artifact and only then
    stamps the classifier_runs row with the real ``runtime_fingerprint``.
    Predictions are written by ``_store_grouped_predictions``, so the
    promotion pass belongs AFTER it — running it earlier (inside
    ``_record_batch_classifier_runs``, before those rows exist) silently
    no-ops and leaves classifier_runs stranded on
    ``runtime_fingerprint = 'legacy'``, excluding those runs from cache
    bundle exports.

    Entries from the reuse path carry ``_existing = True``; their
    runtime_fingerprint is already populated from the earlier successful
    publish, so they are skipped here.

    ``store`` is the caller's configured ``ArtifactStore``. Callers that
    honor ``COMPUTATION_CACHE_DIR`` must forward the job's store here so
    freshly promoted artifacts land in the same cache the status / export /
    catalog-reapplication paths use; passing ``None`` falls back to the
    default ``~/.vireo/computation-cache`` location.
    """
    if not labels_fingerprint_full or not model_identity or not raw_results:
        return
    from computation_cache import promote_and_publish_classifier_run

    seen: set = set()
    for result in raw_results:
        det_id = result.get("detection_id")
        if det_id is None or det_id in seen or result.get("_existing"):
            continue
        seen.add(det_id)
        try:
            input_source = result.get("_input_source")
            promote_and_publish_classifier_run(
                db,
                det_id,
                classifier_model,
                labels_fingerprint,
                labels_fingerprint_full,
                model_identity,
                store=store,
                taxonomy_identity=taxonomy_identity,
                source_is_original=(
                    input_source == "original"
                    if input_source in ("original", "working_copy")
                    else None
                ),
            )
        except Exception:
            log.warning(
                "Could not publish portable classifier result for detection %s",
                det_id, exc_info=True,
            )


def _cached_prediction_taxonomy(row):
    row = dict(row)
    if not row.get("source_taxon_id") and row.get("labels_fingerprint") != "tol":
        return None  # Legacy custom-label scientific names were inferred.
    result = {rank: row.get("taxonomy_" + rank) for rank in (
        "kingdom", "phylum", "class", "order", "family", "genus",
    )}
    result.update(scientific_name=row.get("scientific_name"), taxon_id=row.get("source_taxon_id"))
    return {key: value for key, value in result.items() if value} or None


def _prediction_taxonomy(tax, species, supplied=None):
    """Enrich source-backed labels by binomial, preserving source provenance."""
    supplied = supplied or {}
    from species_identity import SpeciesResolver
    identity = SpeciesResolver(taxonomy=tax).resolve(
        species, supplied.get("scientific_name"), supplied if supplied.get("taxon_id") else None,
    )
    name = identity.scientific_name or (None if supplied.get("taxon_id") else species)
    hierarchy = tax.get_hierarchy(name) if tax and name else {}
    return {**hierarchy, **supplied}


def _store_match_prediction(
    db, item, model_name, labels_fingerprint, tax=None,
    species=None, confidence=None, taxonomy=None,
    store_alternatives=True,
):
    """Persist an already-labeled classifier result as a reusable cache row.

    A taxonomy ``match`` means the photo's XMP already carries this species, so
    it should not re-enter the pending review queue.  The raw classifier output
    still needs a prediction row, though; otherwise the next non-reclassify run
    sees a classifier_runs key with no cached prediction to surface and pays for
    inference again.

    ``store_alternatives`` must be False when ``species``/``confidence`` are a
    consensus override (burst groups): the per-frame ``item["alternatives"]``
    are runner-ups for that frame's own top-1, not for the consensus species,
    and an alternative's confidence can exceed the consensus average. Since
    ``get_predictions_for_detection`` orders ``confidence DESC``, caching them
    would let a per-frame alternative outrank the consensus primary and become
    the cached top-1 on later non-reclassify runs.
    """
    species = species or item["prediction"]
    confidence = item["confidence"] if confidence is None else confidence
    tax_hierarchy = _prediction_taxonomy(tax, species, taxonomy or item.get("taxonomy"))
    db.add_prediction(
        detection_id=item["detection_id"],
        species=species,
        confidence=round(confidence, 4),
        model=model_name,
        category="match",
        status="accepted",
        individual=AUTO_MATCH_REVIEW_MARKER,
        taxonomy=tax_hierarchy,
        labels_fingerprint=labels_fingerprint,
        preserve_manual_review=True,
    )
    if store_alternatives:
        # Skip alternatives whose normalized species collides with the primary
        # (or a previously stored alternative). ``add_prediction`` folds
        # apostrophes centrally, so an active label set with both `Say's
        # Phoebe` and `Say’s Phoebe` (or a classifier that returns both
        # spellings in one call) would resolve every ``alt`` to the primary's
        # unique row. The alternative's re-queried INSERT then upserts
        # ``prediction_review.status = 'alternative'``, silently overwriting
        # the auto-accepted match status (or, for a pending primary via
        # ``_store_pending_detection_prediction``, hiding the top-1 from
        # review). Dedupe on the same normalized key ``add_prediction`` uses
        # so alternatives-that-are-actually-the-primary stay unwritten.
        #
        # Comparison uses ``_species_match_key`` (ASCII-only case fold):
        # downstream keyword joins already use SQLite ``COLLATE NOCASE``
        # (see ``_fold_prediction_species_apostrophes``), so a merged
        # label set yielding primary `Say's Phoebe` and alternative
        # `Say's phoebe` is semantically one bird — but
        # ``_folded_species_key`` preserves case, letting them survive as
        # two BINARY-unique rows with the alternative overwriting the
        # primary's review to ``alternative``. ``str.lower()`` folds
        # non-ASCII case pairs (``Éclair``/``éclair``, ``Maße``/``masse``)
        # that the DB treats as distinct, so it would silently drop a
        # legitimate second alternative.
        seen_species = {_species_match_key(species)}
        for alt in item.get("alternatives", []):
            alt_key = _species_match_key(alt["species"])
            if alt_key in seen_species:
                continue
            seen_species.add(alt_key)
            alt_tax = _prediction_taxonomy(tax, alt["species"], alt.get("taxonomy"))
            db.add_prediction(
                detection_id=item["detection_id"],
                species=alt["species"],
                confidence=round(alt["confidence"], 4),
                model=model_name,
                category="match",
                status="alternative",
                taxonomy=alt_tax,
                labels_fingerprint=labels_fingerprint,
                preserve_manual_review=True,
            )
    # add_prediction is INSERT-OR-IGNORE: a row cached as non-match on an
    # earlier pass keeps its stale category here. Re-stamp it 'match' so the
    # downgrade in reconcile_match_review_state still fires if this detection
    # later stops being a match.
    db.reconcile_match_review_state(
        item["detection_id"], model_name, labels_fingerprint,
        species, "match",
    )


def _recognized_taxon_keywords(keywords, tax):
    """Return XMP keywords that the loaded taxonomy recognizes as taxa."""
    if not tax or not hasattr(tax, "is_taxon"):
        return []
    taxa = []
    seen = set()
    for kw in keywords or []:
        if not kw or not tax.is_taxon(kw):
            continue
        key = kw.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        taxa.append(kw)
    return taxa


def _categorize_detection_prediction(prediction, existing_keywords, tax):
    """Categorize one detection prediction against its photo-level keywords.

    Photo keywords are not detection-scoped, so this only describes whether
    the prediction agrees with the photo's species set. The caller separately
    decides whether a photo-level match is specific enough to auto-accept.
    """
    if not tax:
        return "new"
    from compare import categorize

    return categorize(prediction, existing_keywords, tax)


def _can_auto_accept_detection_prediction(prediction, category, existing_keywords, tax):
    """Return True when a photo-level match is unambiguous for one detection.

    A photo may carry taxonomy-hierarchy keywords (e.g. ``Aves`` alongside
    ``Robin``) without adding a second species: ancestors of the matched
    prediction are just broader labels for the same taxon. A single
    descendant is also fine — it just refines the prediction to a specific
    species. But multiple *distinct* descendants (e.g. the prediction is
    ``Sparrow`` and the sidecar has ``White-crowned Sparrow`` +
    ``Golden-crowned Sparrow``) describe more than one species and must go
    through review, even though ``categorize()`` still returns ``"match"``
    because each keyword is a descendant of the prediction.

    Descendants only fold into the same species when they resolve to one
    another (a species and its own subspecies, for example); any pair that
    is ``sibling``/``unrelated`` means genuinely distinct species and
    forces the detection back to pending review.
    """
    if category != "match":
        return False
    taxa = _recognized_taxon_keywords(existing_keywords, tax)
    if not tax or not hasattr(tax, "relationship"):
        return len(taxa) <= 1
    descendants = []
    for kw in taxa:
        rel = tax.relationship(kw, prediction)
        if rel in ("same", "ancestor"):
            continue
        if rel == "descendant":
            descendants.append(kw)
            continue
        return False
    for i, a in enumerate(descendants):
        for b in descendants[i + 1:]:
            rel = tax.relationship(a, b)
            if rel not in ("same", "ancestor", "descendant"):
                return False
    return True


def _store_pending_detection_prediction(
    db,
    item,
    model_name,
    labels_fingerprint,
    category,
    tax=None,
    group_id=None,
    vote_count=None,
    total_votes=None,
    individual=None,
):
    tax_hierarchy = _prediction_taxonomy(tax, item["prediction"], item.get("taxonomy"))
    if item.get("_existing"):
        existing_row = db.conn.execute(
            """SELECT id FROM predictions
               WHERE detection_id = ? AND classifier_model = ?
                 AND labels_fingerprint = ? AND species IS ?""",
            (
                item["detection_id"],
                model_name,
                labels_fingerprint,
                item["prediction"],
            ),
        ).fetchone()
        db.reconcile_match_review_state(
            item["detection_id"], model_name, labels_fingerprint,
            item["prediction"], category,
            auto_accept=False,
        )
        if existing_row is not None:
            if group_id is not None:
                db.update_prediction_group_info(
                    detection_id=item["detection_id"],
                    model=model_name,
                    group_id=group_id,
                    vote_count=vote_count,
                    total_votes=total_votes,
                    individual=individual,
                    labels_fingerprint=labels_fingerprint,
                )
            else:
                # Cached prediction may still carry group metadata from an
                # earlier grouped burst; if the current run decided this
                # detection is not group-reviewable, drop the stale group_id
                # so ungrouping actually takes effect on reuse.
                db.clear_prediction_group_info(
                    detection_id=item["detection_id"],
                    model=model_name,
                    labels_fingerprint=labels_fingerprint,
                )
            return

    db.add_prediction(
        detection_id=item["detection_id"],
        species=item["prediction"],
        confidence=round(item["confidence"], 4),
        model=model_name,
        category=category,
        group_id=group_id,
        vote_count=vote_count,
        total_votes=total_votes,
        individual=individual,
        taxonomy=tax_hierarchy,
        labels_fingerprint=labels_fingerprint,
    )
    db.reconcile_match_review_state(
        item["detection_id"], model_name, labels_fingerprint,
        item["prediction"], category,
        auto_accept=False,
    )
    # See _store_match_prediction for why alternatives are deduped by the
    # same normalized key that ``add_prediction`` uses (and why the
    # comparison uses the ASCII-only ``_species_match_key`` rather than
    # ``str.lower``): without this, an alternative that folds to the
    # primary — including one that differs only by ASCII capitalization —
    # would upsert the primary's prediction_review row to
    # ``status='alternative'``, hiding the only top-1 prediction from the
    # pending queue.
    seen_species = {_species_match_key(item["prediction"])}
    for alt in item.get("alternatives", []):
        alt_key = _species_match_key(alt["species"])
        if alt_key in seen_species:
            continue
        seen_species.add(alt_key)
        alt_tax = _prediction_taxonomy(tax, alt["species"], alt.get("taxonomy"))
        db.add_prediction(
            detection_id=item["detection_id"],
            species=alt["species"],
            confidence=round(alt["confidence"], 4),
            model=model_name,
            category=category,
            status="alternative",
            taxonomy=alt_tax,
            labels_fingerprint=labels_fingerprint,
        )


def _store_grouped_predictions(
    raw_results, job_id, model_name, grouping_window, similarity_threshold, tax, db,
    labels_fingerprint="legacy",
):
    """Group results by timestamp/similarity, compute consensus, store to DB.

    ``labels_fingerprint`` is written verbatim onto each prediction row so
    the fingerprint-aware skip gate (``get_existing_prediction_photo_ids``)
    actually finds them. Defaulting to ``'legacy'`` would make cache
    lookups miss and force reclassification on every pass — callers must
    pass the active fingerprint.

    Returns:
        dict with predictions_stored, burst_groups, already_labeled counts.
    """
    from grouping import (
        consensus_prediction,
        group_by_timestamp,
        refine_groups_by_similarity,
    )
    from species_identity import SpeciesResolver
    from xmp import read_keywords

    resolver = SpeciesResolver(taxonomy=tax)

    def identity_for(item):
        supplied = item.get("taxonomy") or {}
        source = supplied if supplied.get("taxon_id") else None
        return resolver.resolve(item["prediction"], supplied.get("scientific_name"), source)

    def comparison_name(item):
        identity = identity_for(item)
        return identity.scientific_name or identity.display_name

    groups = group_by_timestamp(raw_results, window_seconds=grouping_window)
    groups = refine_groups_by_similarity(
        groups, similarity_threshold=similarity_threshold
    )
    predictions_stored = 0
    group_count = 0
    skipped_match = 0

    for group in groups:
        if len(group) == 1:
            item = group[0]
            photo = item["photo"]
            folder_path = item["folder_path"]

            category = "new"
            auto_accept = False
            if tax:
                xmp_path = os.path.join(
                    folder_path,
                    os.path.splitext(photo["filename"])[0] + ".xmp",
                )
                existing = read_keywords(xmp_path)
                category = _categorize_detection_prediction(
                    comparison_name(item), existing, tax,
                )
                auto_accept = _can_auto_accept_detection_prediction(
                    comparison_name(item), category, existing, tax,
                )

            if auto_accept:
                _store_match_prediction(
                    db, item, model_name, labels_fingerprint, tax,
                )
                skipped_match += 1
                continue

            _store_pending_detection_prediction(
                db, item, model_name, labels_fingerprint, category, tax,
            )
            predictions_stored += 1
        else:
            group_count += 1
            gid = f"g{job_id[-6:]}-{group_count:04d}"
            # Vote on identity, retaining the first display spelling for
            # each key. Synonymous prompts must not split a unanimous vote.
            display_by_key = {}
            for item in group:
                identity = identity_for(item)
                display_by_key.setdefault(identity.key, _folded_species_key(identity.display_name))

            cons_input = [
                {
                    "prediction": identity_for(item).key,
                    "confidence": item["confidence"],
                }
                for item in group
            ]
            cons = consensus_prediction(cons_input)
            if not cons:
                continue

            group_species = {
                identity_for(item).key
                for item in group
                if item.get("prediction")
            }
            group_reviewable = len(group_species) == 1
            cons["prediction"] = display_by_key[cons["prediction"]]
            individual_json = (
                json.dumps({display_by_key[key]: count for key, count in cons["individual_predictions"].items()})
                if group_reviewable
                else None
            )
            stored_in_group = 0
            for item in group:
                photo = item["photo"]
                category = "new"
                auto_accept = False
                if tax:
                    xmp_path = os.path.join(
                        item["folder_path"],
                        os.path.splitext(photo["filename"])[0] + ".xmp",
                    )
                    existing = read_keywords(xmp_path)
                    category = _categorize_detection_prediction(
                        comparison_name(item), existing, tax,
                    )
                    auto_accept = _can_auto_accept_detection_prediction(
                        comparison_name(item), category, existing, tax,
                    )

                if auto_accept:
                    _store_match_prediction(
                        db, item, model_name, labels_fingerprint, tax,
                        store_alternatives=False,
                    )
                    skipped_match += 1
                    continue

                _store_pending_detection_prediction(
                    db,
                    item,
                    model_name,
                    labels_fingerprint,
                    category,
                    tax,
                    group_id=gid if group_reviewable else None,
                    vote_count=cons["vote_count"] if group_reviewable else None,
                    total_votes=cons["total_votes"] if group_reviewable else None,
                    individual=individual_json,
                )
                stored_in_group += 1

            predictions_stored += stored_in_group

    singles = len([g for g in groups if len(g) == 1])
    grouped_photos = sum(len(g) for g in groups if len(g) > 1)
    log.info(
        "Grouping complete: %d predictions stored (%d singles, %d in %d burst groups), "
        "%d already labeled",
        predictions_stored,
        singles,
        grouped_photos,
        group_count,
        skipped_match,
    )

    return {
        "predictions_stored": predictions_stored,
        "burst_groups": group_count,
        "already_labeled": skipped_match,
    }


def _finalize_remaining_steps(runner, job_id, step_ids, status, summary):
    """Flip not-yet-run step rows to a terminal status before an early return.

    _persist_job stores whatever statuses are in the step tree at job end;
    a row left "pending" with no finished_at renders as an indeterminate
    spinner forever on the jobs page. Early-return paths must call this for
    every step they are about to skip.
    """
    for step_id in step_ids:
        runner.update_step(job_id, step_id, status=status, summary=summary)


def _finalize_cached_only(
    thread_db, runner, job, params, photos,
    classifier_model, labels_fingerprint,
    peek_model=None, peek_labels=None, peek_use_tol=False,
    peek_label_metas=None, peek_succeeded=False,
    detector_confidence=0.0,
):
    """Reconcile imported classifier results without loading a model.

    ``_all_photos_cache_satisfied`` proves every detection already has a
    matching classifier_run row, so no inference is needed. But those
    rows still carry ``category='new'`` from the materialize path and
    lack burst grouping metadata — the ordinary finalize path
    (``_store_grouped_predictions``) performs auto-match reconciliation
    against destination XMP/taxonomy and stamps group_id / vote counts.
    Skipping it silently leaves imported predictions unreconciled, which
    is why the earlier early-return "success" was actually a bug.

    ``peek_*`` — the labels/mode/metadata resolved earlier in
    ``run_classify_job`` before this shortcut fired. ``peek_succeeded`` is
    True when ``_load_labels`` actually returned (as opposed to raising and
    the caller falling back to model-only cache filtering). Used only to
    publish the ``label_source`` line on the classify step, so a re-run
    against a fully cached collection still records which lists produced
    its predictions — otherwise the classify history row is unlabeled,
    exactly the state this feature set out to eliminate.

    Returns the count dict ``run_classify_job`` returns to the runner.
    """
    from datetime import datetime as dt

    from taxonomy import load_local_taxonomy

    total = len(photos)

    runner.update_step(job["id"], "load_taxonomy", status="running")
    tax = load_local_taxonomy()
    runner.update_step(
        job["id"], "load_taxonomy",
        status="completed",
        summary="Taxonomy loaded" if tax else "No taxonomy",
    )
    _finalize_remaining_steps(
        runner, job["id"], ["load_model", "detect"],
        status="completed", summary="Reused cached results",
    )

    # Name the label space on the classify step BEFORE the reconcile pass.
    # Without this, a cached-only run finishes with no label_source line and
    # its history row looks the same as a pre-feature run. ``class_count``
    # is unavailable here — no classifier is constructed — so the two
    # count-dependent branches (Tree of Life exact count, timm exact count)
    # degrade to their generic wording; that is acceptable because the
    # regional-list branch carries its count in ``labels``.
    label_source_text = None
    if peek_succeeded:
        peek_model_type = (peek_model or {}).get("model_type", "bioclip")
        label_source_text = describe_label_source(
            params, thread_db,
            labels=peek_labels,
            use_tol=peek_use_tol,
            model_type=peek_model_type,
            label_metas=peek_label_metas,
        )
    else:
        # ``_load_labels`` raised (e.g. an imported cache on a fresh install
        # whose selected labels or Tree-of-Life artifacts aren't present),
        # so we don't have a live label space to name. The identity gate in
        # ``_all_photos_cache_satisfied`` guarantees a single (model, fp)
        # pair covers every reused row — recover its provenance from
        # ``labels_fingerprints`` so the classify history row still says
        # which list produced the predictions instead of being unlabeled,
        # which was exactly the pre-feature state.
        label_source_text = _describe_cached_label_source(
            thread_db,
            [p["id"] for p in photos],
            classifier_model=classifier_model,
            labels_fingerprint=labels_fingerprint,
            detector_confidence=detector_confidence,
        )
    if label_source_text:
        runner.update_step(
            job["id"], "classify", label_source=label_source_text,
        )

    folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
    photo_ids = [p["id"] for p in photos]
    # Apply the workspace-effective detector_confidence so imported bundles
    # from a machine with a lower threshold don't surface subjects the
    # destination workspace would hide.  The normal (non-cache) classify path
    # filters detections with the same threshold before grouping/review.
    import config as cfg
    effective_cfg = thread_db.get_effective_config(cfg.load())
    det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)
    detection_map = thread_db.get_detections_for_photos(
        photo_ids, min_conf=det_conf_threshold,
    )
    detection_map = {
        photo_id: [
            detection for detection in detections
            if detection.get("detector_model") != "full-image"
            and (detection.get("category") or "animal") == "animal"
        ]
        for photo_id, detections in detection_map.items()
    }
    # Synthetic full-image anchors intentionally have confidence 0, so the
    # positive MegaDetector threshold above hides them. They are nevertheless
    # classifier inputs and must participate in cached-only reconciliation.
    # Fetch them independently at min_conf=0 and merge them back.
    full_image_map = thread_db.get_detections_for_photos(
        photo_ids, min_conf=0, detector_model="full-image",
    )
    for photo_id, detections in full_image_map.items():
        existing = detection_map.setdefault(photo_id, [])
        existing_ids = {detection["id"] for detection in existing}
        existing.extend(
            detection for detection in detections
            if detection["id"] not in existing_ids
        )

    runner.update_step(job["id"], "classify", status="running")

    raw_results = []
    resolved_model = classifier_model
    resolved_fingerprint = labels_fingerprint
    reused_predictions = 0
    for photo in photos:
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        timestamp = None
        if photo["timestamp"]:
            try:
                timestamp = dt.fromisoformat(photo["timestamp"])
            except Exception:
                timestamp = None
        for detection in detection_map.get(photo["id"], []):
            cached = thread_db.get_predictions_for_detection(
                detection["id"],
                classifier_model=classifier_model,
                labels_fingerprint=labels_fingerprint,
                min_classifier_conf=0,
            )
            if not cached:
                continue
            top = cached[0]
            # When the caller didn't request a specific (model, fp), the
            # satisfaction check accepts any classifier's cached row.
            # Adopt the first cached row's identity so the reconcile
            # UPDATE below hits the same rows we consumed from — passing
            # None here would leave the reconcile matching zero rows.
            if resolved_model is None:
                resolved_model = top["classifier_model"]
            if resolved_fingerprint is None:
                resolved_fingerprint = top["labels_fingerprint"]
            raw_results.append({
                "photo": photo,
                "detection_id": detection["id"],
                "folder_path": folder_path,
                "image_path": image_path,
                "prediction": top["species"],
                "confidence": top["confidence"],
                "timestamp": timestamp,
                "filename": photo["filename"],
                "embedding": None,
                "taxonomy": _cached_prediction_taxonomy(top),
                "alternatives": [],
                "_existing": True,
            })
            reused_predictions += 1
    runner.update_step(
        job["id"], "classify",
        status="completed",
        summary=(
            f"{reused_predictions} cached"
            if reused_predictions else "No cached predictions"
        ),
    )

    runner.update_step(job["id"], "finalize", status="running")
    runner.push_event(
        job["id"], "progress",
        {
            "current": total,
            "total": total,
            "current_file": "Grouping bursts and computing consensus...",
            "rate": 0,
            "phase": "Finalizing results",
        },
    )
    group_result = _store_grouped_predictions(
        raw_results=raw_results,
        job_id=job["id"],
        model_name=resolved_model,
        grouping_window=params.grouping_window,
        similarity_threshold=params.similarity_threshold,
        tax=tax,
        db=thread_db,
        labels_fingerprint=resolved_fingerprint or "legacy",
    )
    finalize_parts = [f"{group_result['predictions_stored']} predictions"]
    if group_result["burst_groups"]:
        finalize_parts.append(f"{group_result['burst_groups']} burst groups")
    if group_result["already_labeled"]:
        finalize_parts.append(
            f"{group_result['already_labeled']} already labeled"
        )
    runner.update_step(
        job["id"], "finalize",
        status="completed", summary=", ".join(finalize_parts),
    )
    return {
        "total": total,
        "predictions_stored": group_result["predictions_stored"],
        "burst_groups": group_result["burst_groups"],
        "already_classified": reused_predictions,
        "already_labeled": group_result["already_labeled"],
        "detected": 0,
        "failed": 0,
    }


def run_classify_job(
    job, runner, db_path, workspace_id, params, vireo_dir=None,
    computation_cache_dir=None,
):
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
    classifier_cache_handle = None
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

        # Phase 1: Get photos from collection — runs before model resolution
        # so that a collection fully filtered out by the subject-skip gate
        # short-circuits without ever attempting to load (or fail to load)
        # a classifier model. Otherwise users with no model downloaded would
        # see "No model available" for jobs that have zero work to do.
        runner.update_step(job["id"], "load_photos", status="running")
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": 0,
                "current_file": "Loading collection photos...",
                "rate": 0,
                "phase": "Step 1/5: Loading photos",
            },
        )
        photos = thread_db.get_collection_photos(params.collection_id, per_page=999999)

        photo_ids = [p["id"] for p in photos]
        pre_count = len(photos)
        kept_raw = getattr(
            thread_db, "filter_out_wildlife_excluded", lambda ids: ids
        )(photo_ids)
        if not isinstance(kept_raw, list | tuple | set):
            kept_raw = photo_ids
        kept_ids = set(kept_raw)
        photos = [p for p in photos if p["id"] in kept_ids]
        skipped_wildlife = pre_count - len(photos)
        if skipped_wildlife:
            log.info(
                "Skipping %d photo(s) marked not wildlife",
                skipped_wildlife,
            )
            runner.push_event(
                job["id"], "progress",
                {
                    "current": 0,
                    "total": len(photos),
                    "current_file": (
                        f"Skipped {skipped_wildlife} photo(s) marked not wildlife"
                    ),
                    "rate": 0,
                    "phase": "Step 1/5: Loading photos",
                    "skipped_wildlife_excluded": skipped_wildlife,
                },
            )

        # Skip photos already tagged with a 'subject' keyword (per workspace
        # config). reclassify=True bypasses so users can verify existing tags.
        if not params.reclassify:
            subject_types = thread_db.get_subject_types()
            if subject_types:
                pre_count = len(photos)
                kept_ids = set(thread_db.filter_out_subject_tagged(
                    [p["id"] for p in photos], subject_types,
                ))
                photos = [p for p in photos if p["id"] in kept_ids]
                skipped_subject = pre_count - len(photos)
                if skipped_subject:
                    log.info(
                        "Skipping %d photo(s) with subject keywords (types=%s)",
                        skipped_subject, sorted(subject_types),
                    )
                    runner.push_event(
                        job["id"], "progress",
                        {
                            "current": 0,
                            "total": len(photos),
                            "current_file": (
                                f"Skipped {skipped_subject} already-identified "
                                f"photo(s)"
                            ),
                            "rate": 0,
                            "phase": "Step 1/5: Loading photos",
                            "skipped_subject": skipped_subject,
                        },
                    )

        total = len(photos)
        job["progress"]["total"] = total
        runner.update_step(
            job["id"], "load_photos", status="completed",
            summary=f"{total} photos",
        )

        # If the subject-skip filter (or an empty source collection) left
        # nothing to classify, short-circuit before model resolution. Model
        # resolution can fail with RuntimeError when no model is downloaded,
        # which would surface as a hard error for a job that has no work.
        if total == 0:
            log.info(
                "Classify job: no photos to process after filtering; "
                "skipping model resolution and detection",
            )
            runner.push_event(
                job["id"], "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": "No photos to classify",
                    "rate": 0,
                    "phase": "Step 1/5: Loading photos",
                },
            )
            _finalize_remaining_steps(
                runner, job["id"],
                ["load_taxonomy", "load_model", "detect", "classify",
                 "finalize"],
                status="completed", summary="Skipped (no photos to classify)",
            )
            return {
                "total": 0,
                "predictions_stored": 0,
                "burst_groups": 0,
                "already_classified": 0,
                "already_labeled": 0,
                "detected": 0,
                "failed": 0,
            }

        # Photos may have been cataloged after a bundle import. Apply any
        # matching stored objects before model resolution so their raw results
        # participate in the ordinary database cache gates.
        try:
            from computation_cache import ArtifactStore, materialize_local_store

            cache_store = (
                ArtifactStore(computation_cache_dir)
                if computation_cache_dir else None
            )
            reused = (
                materialize_local_store(thread_db, store=cache_store)
                if not params.reclassify else {}
            )
            if reused.get("detector_runs_applied") or reused.get(
                "classifier_runs_applied"
            ):
                log.info(
                    "Portable cache applied %d detector and %d classifier runs",
                    reused.get("detector_runs_applied", 0),
                    reused.get("classifier_runs_applied", 0),
                )
        except Exception:
            log.warning("Could not apply local computation cache", exc_info=True)

        # Cancellation gate before the expensive phases (model resolution,
        # weight download, inference). The job loops below also check
        # per-photo; _run_job flips the terminal status to 'cancelled'.
        if runner.is_cancelled(job["id"]):
            log.info("Classify job cancelled before model resolution")
            _finalize_remaining_steps(
                runner, job["id"],
                ["load_taxonomy", "load_model", "detect", "classify",
                 "finalize"],
                status="cancelled", summary="Cancelled before start",
            )
            return {
                "total": total,
                "predictions_stored": 0,
                "burst_groups": 0,
                "already_classified": 0,
                "already_labeled": 0,
                "detected": 0,
                "failed": 0,
            }

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
        desired_classifier_model = None
        desired_labels_fingerprint = None
        peek_model = None
        # ``model_id_missing`` distinguishes "the requested model really is
        # gone from the catalog" (a stale job referencing a deleted model)
        # from a transient ``get_models()`` failure. The catch-all below
        # nulls out the transient case; a bare miss stays flagged so we
        # can raise the definitive error before the cache shortcut.
        model_id_missing = False
        try:
            if params.model_id:
                peek_model = next(
                    (m for m in get_models() if m["id"] == params.model_id),
                    None,
                )
                if peek_model:
                    # Preserve the explicitly requested model identity
                    # even when the weights aren't downloaded yet.
                    # Otherwise ``_all_photos_cache_satisfied`` would
                    # treat any classifier's cached runs as valid for
                    # this explicit request and silently return
                    # wrong-model results on a fresh install.
                    desired_classifier_model = (
                        params.model_name or peek_model.get("name")
                    )
                else:
                    model_id_missing = True
            elif params.model_name:
                desired_classifier_model = params.model_name
                peek_model = next(
                    (
                        m for m in get_models()
                        if m.get("name") == params.model_name
                    ),
                    None,
                )
            else:
                peek_model = get_active_model()
                if peek_model:
                    desired_classifier_model = (
                        params.model_name or peek_model.get("name")
                    )
        except Exception:
            # A raising get_models() here left both peek and constraints
            # unresolved. Clearing ``model_id_missing`` or
            # ``desired_classifier_model`` unconditionally would let
            # ``_all_photos_cache_satisfied`` silently accept cached
            # runs from a classifier the caller did not select. Preserve
            # the explicit request so either the guard below propagates
            # the lookup failure or the cache filter still rejects
            # wrong-model runs.
            peek_model = None
            if params.model_id:
                desired_classifier_model = None
                model_id_missing = True
            elif params.model_name:
                desired_classifier_model = params.model_name
                model_id_missing = False
            else:
                desired_classifier_model = None
                model_id_missing = False

        # An unknown model_id must never fall through to the cache
        # shortcut. Without this, ``desired_classifier_model`` stays
        # ``None`` and ``_all_photos_cache_satisfied`` accepts runs from
        # any classifier, silently reporting success for a request that
        # would otherwise raise "not found or not downloaded" below.
        if model_id_missing:
            raise RuntimeError(
                f"Model '{params.model_id}' not found or not downloaded."
            )
        # Compute labels_fingerprint from the user-selected label sources
        # so a cache built from a DIFFERENT label set does not silently
        # pass as satisfied.  Uses the same _load_labels path that later
        # produces the authoritative fingerprint, but tolerates failure
        # (missing weights on a fresh install, TOL path unavailable) —
        # in that case desired_labels_fingerprint stays None and the
        # check falls back to model-only filtering.
        desired_labels_fingerprint_full = None
        peek_labels = None
        peek_use_tol = False
        peek_label_metas: list[dict] = []
        peek_succeeded = False
        try:
            from labels_fingerprint import (
                compute_fingerprint,
                compute_full_fingerprint,
            )

            peek_labels, peek_use_tol, peek_label_metas = _load_labels(
                model_type=(peek_model or {}).get("model_type", "bioclip"),
                model_str=(peek_model or {}).get("model_str", ""),
                labels_file=params.labels_file,
                labels_files=params.labels_files,
                db=thread_db,
                model_dir=(peek_model or {}).get("weights_path"),
            )
            peek_succeeded = True
            desired_labels_fingerprint = compute_fingerprint(peek_labels)
            fp_full_peek = compute_full_fingerprint(peek_labels)
            if isinstance(fp_full_peek, str) and len(fp_full_peek) == 64:
                desired_labels_fingerprint_full = fp_full_peek
        except Exception:
            desired_labels_fingerprint = None

        # Resolve the installed classifier's portable identity so the
        # cache check can filter classifier_runs by runtime_fingerprint.
        # Without this, a stale run (weights or preprocessing changed
        # under the same classifier_model, unchanged label fingerprint)
        # still satisfies the coverage join and the caller shortcuts to
        # ``_finalize_cached_only`` on wrong-runtime results.  Falling
        # back to ``None`` keeps the fresh-install case (undownloaded
        # weights, missing revision file) working as before.
        desired_model_identity = None
        try:
            from computation_cache import classifier_model_identity
            from computation_cache import fingerprint as identity_fingerprint

            if peek_model and peek_model.get("weights_path"):
                desired_model_identity = classifier_model_identity(peek_model)
            if (
                desired_labels_fingerprint_full is None
                and peek_use_tol
                and desired_model_identity
            ):
                # Tree-of-Life mode uses a synthetic labels_full that
                # does not require ``peek_labels`` to be non-empty.
                # Mirrors the fp_full fallback later in the job.
                desired_labels_fingerprint_full = identity_fingerprint({
                    "label_space": "tree-of-life",
                    "model": desired_model_identity,
                })
        except (OSError, ValueError):
            desired_model_identity = None

        import config as cfg

        cache_effective_cfg = thread_db.get_effective_config(cfg.load())
        cache_detector_confidence = cache_effective_cfg.get(
            "detector_confidence", 0.2,
        )
        # Peek the local taxonomy identity so the expected classifier
        # runtimes computed inside ``_all_photos_cache_satisfied`` include
        # the taxonomy axis. Otherwise the check would compute expected
        # runtimes with "no-tax" while installed classifier_runs carry
        # the real taxonomy digest — the cached-only shortcut would then
        # never fire on installs that use taxonomy. ``load_local_taxonomy``
        # is cached, so the actual load in Phase 2 below is free.
        from computation_cache import (
            local_taxonomy_identity as _peek_tax_identity,
        )

        cache_taxonomy_identity = _peek_tax_identity()

        # Re-arm any pending background self-heal before the cached-only
        # shortcut returns. The shortcut below skips classifier acquisition
        # entirely, so ``acquire_cached_classifier``'s ``notify_reuse``
        # hook — the only place the timm ``label_descriptions.json``
        # bounded-retry state machine fires from cache hits — never runs
        # on such jobs. Without this, an installation whose first heal
        # failed during a transient outage would stay ``failed`` for the
        # rest of the process's life whenever every job's photos are
        # already cached, leaking raw scientific names indefinitely.
        # The helper checks the file itself and no-ops when the mapping
        # is already usable or the state machine is in-flight/backing off,
        # so calling it here is cheap on healthy installs.
        if peek_model and peek_model.get("model_type") == "timm":
            _heal_model_str = peek_model.get("model_str")
            _heal_model_dir = peek_model.get("weights_path")
            if _heal_model_str and _heal_model_dir:
                try:
                    from timm_classifier import (
                        rearm_pending_label_desc_heal,
                    )
                    rearm_pending_label_desc_heal(
                        _heal_model_str, _heal_model_dir,
                    )
                except Exception:
                    log.info(
                        "Re-arming label_descriptions heal for %s failed",
                        _heal_model_str, exc_info=True,
                    )

        if not params.reclassify and _all_photos_cache_satisfied(
            thread_db, [p["id"] for p in photos],
            classifier_model=desired_classifier_model,
            labels_fingerprint=desired_labels_fingerprint,
            detector_confidence=cache_detector_confidence,
            model_identity=desired_model_identity,
            labels_fingerprint_full=desired_labels_fingerprint_full,
            taxonomy_identity=cache_taxonomy_identity,
        ):
            log.info(
                "Classify job: every photo has cached classifier runs — "
                "skipping model resolution and detection",
            )
            # Route reused rows through the ordinary finalize path so
            # imported predictions get auto-match reconciliation against
            # destination XMP/taxonomy and burst grouping metadata, which
            # the earlier early-return silently skipped.
            return _finalize_cached_only(
                thread_db, runner, job, params, photos,
                classifier_model=desired_classifier_model,
                labels_fingerprint=desired_labels_fingerprint,
                peek_model=peek_model,
                peek_labels=peek_labels,
                peek_use_tol=peek_use_tol,
                peek_label_metas=peek_label_metas,
                peek_succeeded=peek_succeeded,
                detector_confidence=cache_detector_confidence,
            )

        # Resolve model (deferred until we know there is work to do)
        if params.model_id:
            all_models = get_models()
            active_model = next(
                (m for m in all_models if m["id"] == params.model_id and m["downloaded"]),
                None,
            )
            if not active_model:
                raise RuntimeError(
                    f"Model '{params.model_id}' not found or not downloaded."
                )
        else:
            active_model = get_active_model()
        if not active_model:
            raise RuntimeError("No model available. Download one in Settings.")

        model_str = active_model["model_str"]
        weights_path = active_model["weights_path"]
        effective_name = active_model["name"]
        model_type = active_model.get("model_type", "bioclip")
        model_name = params.model_name or effective_name

        folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}

        # Phase 2: Load taxonomy
        runner.update_step(job["id"], "load_taxonomy", status="running")
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": "Loading taxonomy...",
                "rate": 0,
                "phase": "Step 2/5: Loading taxonomy",
            },
        )
        from taxonomy import load_local_taxonomy
        tax = load_local_taxonomy()

        # Phase 3: Load labels (uses model_type/model_str from above)
        labels, use_tol, label_metas = _load_labels(
            model_type=model_type,
            model_str=model_str,
            labels_file=params.labels_file,
            labels_files=params.labels_files,
            db=thread_db,
            model_dir=weights_path,
        )
        # Compute a content-addressable fingerprint for the active label set.
        # Kept in scope so downstream classifier_runs writes can record the
        # exact (classifier_model, labels_fingerprint) that produced a result.
        from labels_fingerprint import compute_fingerprint, compute_full_fingerprint
        fp = compute_fingerprint(labels)
        fp_full = compute_full_fingerprint(labels)
        if len(fp_full) != 64:
            fp_full = None
        try:
            from computation_cache import classifier_model_identity, fingerprint

            classifier_identity = classifier_model_identity(active_model)
            if fp_full is None and use_tol and classifier_identity:
                fp_full = fingerprint({
                    "label_space": "tree-of-life",
                    "model": classifier_identity,
                })
        except (OSError, ValueError):
            classifier_identity = None
        # Derive the fingerprint's source paths from the metadata
        # ``_load_labels`` actually consumed, not by re-resolving from
        # ``params`` a second time. If a configured labels_file was deleted
        # after the classifier initialized, the fresh resolve would name
        # the stale path while ``labels`` came from the workspace fallback.
        label_sources = _sources_from_metas(label_metas)
        _record_labels_fingerprint(
            thread_db, fp, labels, sources=label_sources,
            full_fingerprint=fp_full,
        )
        job["_labels_fingerprint_full"] = fp_full
        job["_classifier_model_identity"] = classifier_identity
        from computation_cache import taxonomy_identity as _taxonomy_identity
        job["_taxonomy_identity"] = _taxonomy_identity(tax)

        tax_summary = "Taxonomy loaded" if tax else "No taxonomy"
        labels_summary = f"{len(labels)} labels" if labels else ("Tree of Life" if use_tol else "no labels")
        runner.update_step(
            job["id"], "load_taxonomy", status="completed",
            summary=f"{tax_summary}, {labels_summary}",
        )

        log.info(
            "Classifying %d photos with '%s' (%s)", total, effective_name, model_str
        )

        # Phase 4: Initialize classifier
        # The reclassify purge (destructive clears of detections + predictions +
        # cascaded review state) is deferred until AFTER the classifier
        # initializes. Running it before model load means any weight-load
        # failure leaves affected photos with no predictions AND no
        # detections AND no replacement results — shared-folder workspaces
        # lose their cached state too. Deferring preserves the cache on
        # setup failure; users see a clean error and their workspace is
        # unchanged.
        runner.update_step(job["id"], "load_model", status="running")
        if model_type == "timm":
            phase_msg = f"Loading {effective_name} timm model..."
        elif use_tol:
            phase_msg = f"Loading {effective_name} Tree of Life classifier..."
        else:
            phase_msg = f"Loading {effective_name} model and computing label embeddings..."

        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": total,
                "current_file": phase_msg,
                "rate": 0,
                "phase": "Step 3/5: Loading model",
            },
        )

        # Pure-cancel probe for the classifier factory. The factory runs
        # under ``ModelCache._Entry.load_lock``; parking there on pause
        # would strand every concurrent unpaused sibling that acquires
        # the same cache key until Resume (they poll the load_lock at
        # ``model_cache.py:198`` without their own pause boundary). Using
        # the non-parking probe lets construction continue through a
        # pause request — the lock releases as soon as the factory
        # returns, then the outer classify loop honors the pause at the
        # next boundary. Cancel still aborts immediately.
        def _factory_cancel_check():
            probe = getattr(runner, "cancellation_requested", None)
            if probe is not None:
                return probe(job["id"])
            return runner.is_cancelled(job["id"])

        # Non-parking pause probe for the same factory. When it reports a
        # pause, the classifier checkpoints the label embeddings finished
        # so far and raises ``ClassifierLoadPaused`` instead of parking
        # under ``load_lock``; the retry loop below parks at this job's
        # own boundary and re-enters construction on Resume.
        def _factory_pause_check():
            probe = getattr(runner, "pause_requested", None)
            return bool(probe is not None and probe(job["id"]))

        if model_type == "timm":
            if runner.is_cancelled(job["id"]):
                runner.update_step(
                    job["id"], "load_model",
                    status="cancelled", summary="Cancelled",
                )
                _finalize_remaining_steps(
                    runner, job["id"], ["detect", "classify", "finalize"],
                    status="cancelled", summary="Cancelled before start",
                )
                return {
                    "total": total,
                    "predictions_stored": 0,
                    "burst_groups": 0,
                    "already_classified": 0,
                    "already_labeled": 0,
                    "detected": 0,
                    "failed": 0,
                }
            def _construct_classifier():
                if _factory_cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                return TimmClassifier(model_str, taxonomy=tax)
        else:
            def _emb_progress(current, emb_total):
                runner.update_step(
                    job["id"], "load_model",
                    progress={"current": current, "total": emb_total},
                )
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": emb_total,
                        "current_file": f"Computing label embeddings ({current}/{emb_total})...",
                        "rate": 0,
                    "phase": "Step 3/5: Computing embeddings",
                    },
                )

            def _construct_classifier():
                if _factory_cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                if not use_tol:
                    _reuse_saved_label_embeddings(thread_db, model_str, weights_path, labels, _factory_cancel_check)
                return Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                    embedding_progress_callback=_emb_progress,
                    cancel_check=_factory_cancel_check,
                    pause_check=_factory_pause_check,
                )

        try:
            from classifier import ClassifierLoadPaused
            from classifier_cache import acquire_cached_classifier
            from computation_cache import taxonomy_identity
            from resource_ledger import ResourceWaitCancelled

            while True:
                try:
                    classifier_cache_handle = acquire_cached_classifier(
                        model_type=model_type,
                        model_str=model_str,
                        weights_path=weights_path,
                        labels=None if use_tol else labels,
                        factory=_construct_classifier,
                        files=active_model.get("files"),
                        # Optional-artifact presence must flip the
                        # fingerprint too. timm declares
                        # label_descriptions.json as optional and
                        # TimmClassifier reads it to translate to common
                        # names, so after a Repair downloads it we must
                        # not reuse the pre-repair classifier still
                        # emitting scientific names. bioclip-2.5's ToL
                        # artifacts are declared the same way.
                        optional_files=active_model.get("optional_files"),
                        taxonomy_fingerprint=(
                            taxonomy_identity(tax)
                            if model_type == "timm" else None
                        ),
                        cancel_check=_factory_cancel_check,
                    )
                    clf = classifier_cache_handle.__enter__()
                    break
                except ClassifierLoadPaused:
                    # The factory stepped out of the shared load lock with
                    # its label embeddings checkpointed. Park here, where
                    # no lock is held, then construct again: the resumed
                    # computation continues from the checkpoint. A Cancel
                    # during the pause surfaces on the retry through the
                    # factory's own cancel probe.
                    log.info(
                        "Classifier load paused; parking until Resume",
                    )
                    runner.is_cancelled(job["id"])
                    continue
        except (ClassificationCancelled, ResourceWaitCancelled):
            # Two cancellation shapes reach here: ``ClassificationCancelled``
            # from ``ModelCache``'s waiter-local cancel probe, and
            # ``ResourceWaitCancelled`` from the ONNX construction lease
            # inside ``create_session`` (``onnx_runtime.py:337``) when
            # the job-bound resource cancel probe fires mid-load. Both
            # mean the same thing to this job: cancel arrived before
            # inference could start. Finalize the step tree the same
            # way so JobRunner does not persist ``load_model`` as still
            # running with the later steps stuck at ``pending``.
            runner.update_step(
                job["id"], "load_model",
                status="cancelled", summary="Cancelled",
            )
            _finalize_remaining_steps(
                runner, job["id"], ["detect", "classify", "finalize"],
                status="cancelled", summary="Cancelled before start",
            )
            return {
                "total": total,
                "predictions_stored": 0,
                "burst_groups": 0,
                "already_classified": 0,
                "already_labeled": 0,
                "detected": 0,
                "failed": 0,
            }
        runner.update_step(
            job["id"], "load_model", status="completed",
            summary=effective_name,
        )

        # Name the label space on the classify step: "Classify species" alone
        # doesn't say whether these photos are being matched against the
        # active regional lists, the full Tree of Life, or a timm model's
        # fixed head — and that decides what the predictions can even be.
        # Pass ``label_metas`` — the metadata ``_load_labels`` actually
        # consumed — so the displayed name matches ``labels`` even if the
        # on-disk sources shifted between here and rendering.
        label_source_text = describe_label_source(
            params, thread_db,
            labels=labels,
            use_tol=use_tol,
            model_type=model_type,
            class_count=getattr(clf, "label_space_size", None),
            label_metas=label_metas,
        )
        if label_source_text:
            runner.update_step(
                job["id"], "classify", label_source=label_source_text,
            )

        # Restamp the portable model identity with the
        # label_descriptions.json this classifier actually consumed.
        # ``classifier_model_identity`` above probed the disk before the
        # classifier was built, and TimmClassifier's background heal
        # publishes that file during normal operation — so the probe can
        # disagree with what the instance read in either direction. Runs
        # published below must carry the mapping that produced their
        # species names; otherwise a pre-heal run is recorded under the
        # post-heal identity and every later cache check accepts it,
        # skipping the reclassify that replaces raw binomials.
        from computation_cache import with_consumed_label_descriptions
        classifier_identity = with_consumed_label_descriptions(
            classifier_identity, clf,
        )
        job["_classifier_model_identity"] = classifier_identity

        # Classifier init succeeded — now it's safe to start the
        # reclassify purge for any failure before this point would leave
        # the cache intact (see comment at the top of this function).
        #
        # The actual destructive clears happen per-photo inside
        # ``_detect_subjects`` rather than upfront for the whole scope.
        # An upfront clear over the full collection meant a mid-detection
        # cancel left the unprocessed tail with both predictions and
        # detections wiped, since the post-detection return gates
        # classification off too. Doing the clear immediately before each
        # photo's re-detection means cancelled photos retain their old
        # state intact.
        #
        # This pre-detection cancel gate still applies: a cancel that
        # landed during taxonomy/label/model init would otherwise advance
        # to the detection loop's per-photo clear and start wiping rows
        # before the user's cancel takes effect.
        if runner.is_cancelled(job["id"]):
            log.info("Classify job cancelled before reclassify purge")
            _finalize_remaining_steps(
                runner, job["id"], ["detect", "classify", "finalize"],
                status="cancelled", summary="Cancelled before start",
            )
            return {
                "total": total,
                "predictions_stored": 0,
                "burst_groups": 0,
                "already_classified": 0,
                "already_labeled": 0,
                "detected": 0,
                "failed": 0,
            }

        # Phase 5: Detect subjects
        runner.update_step(job["id"], "detect", status="running")
        detection_map, detected = _detect_subjects(
            photos=photos,
            folders=folders,
            runner=runner,
            job=job,
            reclassify=params.reclassify,
            db=thread_db,
        )
        cancelled_after_detect = runner.is_cancelled(job["id"])
        # In reclassify mode, ``_detect_subjects`` clears each processed
        # photo's prior detections before re-running detection — and that
        # cascade-deletes its old predictions. Bailing out here would leave
        # the processed subset with fresh detections but zero predictions
        # until a manual rerun. Fall through to classify just that subset
        # so cancel preserves the rebuild work already in flight. The
        # unprocessed tail is dropped — its old state was never touched,
        # so dropping it keeps the cached predictions intact.
        #
        # The processed subset comes from ``job["_detect_processed_ids"]``,
        # which is the union of (a) photos with at least one detection and
        # (b) empty-scene photos that recorded a detector_runs row but
        # added nothing to ``detection_map``. Using ``detection_map``
        # alone would miss case (b), stranding empty-scene reclassified
        # photos with cleared predictions and no replacement. Older test
        # fakes for ``_detect_subjects`` that don't stash this key fall
        # back to ``detection_map.keys()`` for backwards compatibility.
        finish_cleared_only = False
        if cancelled_after_detect:
            runner.update_step(
                job["id"], "detect", status="cancelled",
                summary=f"Cancelled ({detected} animals detected so far)",
            )
            processed_ids = job.get("_detect_processed_ids")
            if processed_ids is None:
                processed_ids = set(detection_map.keys())
            else:
                processed_ids = set(processed_ids) | set(detection_map.keys())
            if params.reclassify and processed_ids:
                processed = [p for p in photos if p["id"] in processed_ids]
                if processed:
                    finish_cleared_only = True
                    photos = processed
                    total = len(photos)
                    job["progress"]["total"] = total
                else:
                    _finalize_remaining_steps(
                        runner, job["id"], ["classify", "finalize"],
                        status="cancelled", summary="Cancelled",
                    )
                    return {
                        "total": total,
                        "predictions_stored": 0,
                        "burst_groups": 0,
                        "already_classified": 0,
                        "already_labeled": 0,
                        "detected": detected,
                        "failed": 0,
                    }
            else:
                _finalize_remaining_steps(
                    runner, job["id"], ["classify", "finalize"],
                    status="cancelled", summary="Cancelled",
                )
                return {
                    "total": total,
                    "predictions_stored": 0,
                    "burst_groups": 0,
                    "already_classified": 0,
                    "already_labeled": 0,
                    "detected": detected,
                    "failed": 0,
                }
        else:
            runner.update_step(
                job["id"], "detect", status="completed",
                summary=f"{detected} animals detected in {total} photos",
            )

        # Reapply the local computation cache now that detection has run.
        # Classification artifacts whose detector dependency was absent at
        # the pre-detection materialize call get a second chance to land
        # here, so a bundle containing only classifications still populates
        # predictions instead of being silently dropped.
        #
        # We ALSO pre-create synthetic full-image detector rows for every
        # empty-scene photo before this reapply.  The classify loop below
        # creates those rows lazily per-photo, so without pre-creating
        # them here a cached full_image classification artifact has no
        # anchor to attach to at reapply time — the classify loop then
        # runs the classifier itself (or, on a fresh machine, fails with
        # "No model available") even though the answer is already sitting
        # in the local store.
        if not params.reclassify:
            try:
                from computation_cache import (
                    full_image_runtime_fingerprint,
                    materialize_local_store,
                    megadetector_runtime_fingerprint,
                    source_input,
                )

                try:
                    local_runtime = megadetector_runtime_fingerprint()
                except (OSError, ValueError):
                    local_runtime = None
                known_runtimes = {full_image_runtime_fingerprint()}
                if local_runtime:
                    known_runtimes.add(local_runtime)

                # Pre-create full-image anchors for empty-scene photos.
                # For photos that already carry a legacy full-image
                # detection (upgraded catalogs where the detection
                # predates the portable-runtime columns), also promote
                # the ``detector_runs`` row to the current portable
                # runtime.  Without the promotion, materialize's
                # classifier gate would defer any imported full-image
                # classification because the stored runtime fingerprint
                # doesn't match ``full_runtime`` — and the classify
                # loop would fall back to real inference (or fail with
                # "No model available" on a fresh machine).
                full_runtime = full_image_runtime_fingerprint()
                empty_scene_ids = [
                    photo["id"] for photo in photos
                    if not detection_map.get(photo["id"])
                ]
                # Import CacheFormatError locally so a NULL / non-canonical
                # file_hash escapes into full_input=None instead of the
                # outer except Exception (which would abandon reapply).
                from computation_cache import CacheFormatError
                for photo_id in empty_scene_ids:
                    identity = thread_db.conn.execute(
                        """SELECT file_hash, companion_path
                           FROM photos WHERE id = ?""",
                        (photo_id,),
                    ).fetchone()
                    full_input = None
                    if identity is not None and not identity["companion_path"]:
                        try:
                            _block, full_input = source_input(
                                identity["file_hash"],
                                "vireo-detector-source-v1",
                            )
                        except (ValueError, CacheFormatError):
                            full_input = None
                    existing_full = thread_db.get_detections(
                        photo_id, detector_model="full-image", min_conf=0,
                    )
                    if not existing_full:
                        thread_db.save_detections(
                            photo_id,
                            [{
                                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                "confidence": 0,
                                "category": "animal",
                            }],
                            detector_model="full-image",
                            runtime_fingerprint=full_runtime,
                        )
                    else:
                        # Promote a legacy full-image detection row so its
                        # ``runtime_fingerprint`` matches the run we're
                        # about to record.  ``exportable_artifacts`` reads
                        # the detector runtime from ``detections`` — if we
                        # leave the row at ``'legacy'`` an export skips
                        # the attached classifier run and emits an empty
                        # full-image detection artifact for this photo.
                        thread_db.conn.execute(
                            """UPDATE detections
                                  SET runtime_fingerprint = ?
                                WHERE photo_id = ?
                                  AND detector_model = 'full-image'
                                  AND (runtime_fingerprint IS NULL
                                       OR runtime_fingerprint != ?)""",
                            (full_runtime, photo_id, full_runtime),
                        )
                        thread_db.conn.commit()
                    existing_run = thread_db.conn.execute(
                        """SELECT runtime_fingerprint FROM detector_runs
                           WHERE photo_id = ?
                             AND detector_model = 'full-image'""",
                        (photo_id,),
                    ).fetchone()
                    if (
                        existing_run is None
                        or existing_run["runtime_fingerprint"] != full_runtime
                    ):
                        thread_db.record_detector_run(
                            photo_id, "full-image", box_count=1,
                            runtime_fingerprint=full_runtime,
                            input_fingerprint=full_input,
                        )

                # Compute the classifier runtimes this job would produce
                # so cached classifications from other machines that
                # match are accepted at reapply time. Without this the
                # classifier-runtime quarantine would drop them even
                # though this install just proved it can reproduce the
                # exact same runtime.
                known_classifier_runtimes = set()
                job_tax_identity = job.get("_taxonomy_identity", "no-tax")
                if fp_full and classifier_identity:
                    try:
                        from computation_cache import (
                            classifier_runtime_fingerprint,
                        )

                        for det_runtime in (local_runtime, full_runtime):
                            if not det_runtime:
                                continue
                            crt = classifier_runtime_fingerprint(
                                classifier_identity, fp_full, det_runtime,
                                taxonomy_identity=job_tax_identity,
                            )
                            if crt:
                                known_classifier_runtimes.add(crt)
                    except Exception:
                        known_classifier_runtimes = set()

                from computation_cache import ArtifactStore

                reapply_store = (
                    ArtifactStore(computation_cache_dir)
                    if computation_cache_dir else None
                )
                reapplied = materialize_local_store(
                    thread_db,
                    store=reapply_store,
                    known_runtimes=known_runtimes,
                    known_classifier_runtimes=(
                        known_classifier_runtimes or None
                    ),
                )
                if reapplied.get("classifier_runs_applied"):
                    log.info(
                        "Portable cache added %d classifier runs after detect",
                        reapplied["classifier_runs_applied"],
                    )
            except Exception:
                log.warning(
                    "Could not reapply local computation cache after detect",
                    exc_info=True,
                )

        # Phase 6: Classify each photo. The per-detection classifier_runs
        # gate inside _classify_photos skips already-done detections and
        # still surfaces their cached predictions into raw_results, so a
        # photo-level short-circuit is both unnecessary and actively
        # harmful (it hides newly-surfaced detections after the user
        # lowers detector_confidence).
        existing_preds = set()

        job["_start_time"] = time.time()  # reset rate timer for classification phase

        import config as cfg
        effective_cfg = thread_db.get_effective_config(cfg.load())
        top_k = effective_cfg.get("top_k_predictions", 5)

        runner.update_step(job["id"], "classify", status="running")
        # ``finish_cleared_only`` runs after Cancel deliberately: detection
        # already cascaded away the old predictions, so this pass rebuilds
        # them to avoid leaving the processed subset empty. The bound
        # cancellation probe is already True here, and CPU inference now
        # consults it, so leaving the binding in place would make every
        # ``_flush_batch`` raise ``ResourceWaitCancelled`` and drop the
        # replacement predictions. Suspend the binding for this preservation
        # pass so inference completes; the JobRunner still owns any hard
        # shutdown signal via the runner-side deadline.
        from resource_ledger import bind_resource_cancel_check
        cancel_binding = (
            bind_resource_cancel_check(None)
            if finish_cleared_only
            else contextlib.nullcontext()
        )
        with cancel_binding:
            raw_results, failed, skipped_existing = _classify_photos(
                photos=photos,
                folders=folders,
                detection_map=detection_map,
                existing_preds=existing_preds,
                clf=clf,
                model_type=model_type,
                model_name=model_name,
                runner=runner,
                job=job,
                db=thread_db,
                top_k=top_k,
                vireo_dir=vireo_dir,
                labels_fingerprint=fp,
                reclassify=params.reclassify,
                finish_cleared_only=finish_cleared_only,
            )
        classified_count = len(raw_results) - skipped_existing
        parts = [f"{classified_count} classified"]
        if skipped_existing:
            parts.append(f"{skipped_existing} cached")
        if failed:
            parts.append(f"{failed} failed")
        cancelled_mid_classify = runner.is_cancelled(job["id"])
        if cancelled_mid_classify:
            # Fall through to finalize: raw_results holds real classifications
            # for the photos completed before the cancel — storing them
            # preserves that work (and matches the per-detection cache, which
            # is already committed).
            if finish_cleared_only:
                summary = (
                    "Cancelled mid-detect — finished "
                    + ", ".join(parts)
                    + " for already-cleared photos"
                )
            else:
                summary = "Cancelled (" + ", ".join(parts) + ")"
            runner.update_step(
                job["id"], "classify", status="cancelled",
                summary=summary,
            )
        else:
            runner.update_step(
                job["id"], "classify", status="completed",
                summary=", ".join(parts),
            )

        # Phase 7: Group and store predictions
        runner.update_step(job["id"], "finalize", status="running")
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": total,
                "total": total,
                "current_file": "Grouping bursts and computing consensus...",
                "rate": 0,
                "phase": "Finalizing results",
            },
        )

        group_result = _store_grouped_predictions(
            raw_results=raw_results,
            job_id=job["id"],
            model_name=model_name,
            grouping_window=params.grouping_window,
            similarity_threshold=params.similarity_threshold,
            tax=tax,
            db=thread_db,
            labels_fingerprint=fp,
        )
        # promote_and_publish reads persisted predictions written by
        # _store_grouped_predictions above; running it inside
        # _record_batch_classifier_runs would no-op because those rows
        # don't exist yet, leaving fresh classifier_runs stranded on
        # runtime_fingerprint = 'legacy' and out of bundle exports.
        # Reconstruct the configured ArtifactStore from the path stashed
        # by ``run_classify_job`` so the classifier artifacts land in the
        # same cache the status / export paths use, matching
        # ``publish_detection_artifact``'s behavior above.
        from computation_cache import ArtifactStore
        _publish_cache_dir = job.get("_computation_cache_dir")
        _publish_store = (
            ArtifactStore(_publish_cache_dir) if _publish_cache_dir else None
        )
        _publish_classifier_runs_for_raw_results(
            thread_db, raw_results, model_name, fp,
            labels_fingerprint_full=job.get("_labels_fingerprint_full"),
            model_identity=job.get("_classifier_model_identity"),
            taxonomy_identity=job.get("_taxonomy_identity", "no-tax"),
            store=_publish_store,
        )
        finalize_parts = [f"{group_result['predictions_stored']} predictions"]
        if group_result["burst_groups"]:
            finalize_parts.append(f"{group_result['burst_groups']} burst groups")
        if group_result["already_labeled"]:
            finalize_parts.append(f"{group_result['already_labeled']} already labeled")
        runner.update_step(
            job["id"], "finalize", status="completed",
            summary=", ".join(finalize_parts),
        )

        log.info(
            "Classification complete: %d photos processed, %d predictions stored, "
            "%d already classified, %d already labeled, %d failed",
            total,
            group_result["predictions_stored"],
            skipped_existing,
            group_result["already_labeled"],
            failed,
        )

        return {
            "total": total,
            "predictions_stored": group_result["predictions_stored"],
            "burst_groups": group_result["burst_groups"],
            "already_classified": skipped_existing,
            "already_labeled": group_result["already_labeled"],
            "detected": detected,
            "failed": failed,
        }
    finally:
        if classifier_cache_handle is not None:
            classifier_cache_handle.release()
        thread_db.conn.close()
