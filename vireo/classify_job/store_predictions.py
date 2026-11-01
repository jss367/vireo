"""Storing a classify run's predictions.

Groups results into bursts, computes consensus, and stores each prediction as
an auto-accepted match or a pending review item, then releases the run's
reserved classifier runs.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING, Any, cast

from classify_job.common import log
from db import AUTO_MATCH_REVIEW_MARKER
from keyword_normalization import folded_species_key as _folded_species_key
from keyword_normalization import species_match_key as _species_match_key
from services import detection_store

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Mapping

    from classify_job.classify_types import RawResult
    from db import Database
    from species_identity import SpeciesIdentity
    from taxonomy import Taxonomy

# ``_folded_species_key`` / ``_species_match_key`` live in
# ``keyword_normalization`` with the rest of the name folds. The private
# aliases are kept because the grouping code and its tests read better with
# the leading underscore signalling "this module's rule".


def _record_match_scores(db: Database, raw_results: list[RawResult], model_name: str, labels_fingerprint: str) -> None:
    """Persist the per-run match summaries collected during classification.

    Runs before the grouping/consensus pass stores predictions, and covers
    every raw result — including ones that pass will discard as already
    labeled. The match score describes the classifier run, not the review
    outcome, so it should not inherit review-side filtering.
    """
    seen = set()
    for result in raw_results:
        det_id = result.get("detection_id")
        match = result.get("match")
        if det_id is None or not match or det_id in seen:
            continue
        seen.add(det_id)
        try:
            # Batch boundary: one detection's match summary.
            with db.transaction(immediate=result.get("_reviewer_box") is not None):
                if not detection_store.reviewer_result_is_current(db, result):
                    continue
                db.model_runs.record_classifier_match_score(
                    det_id,
                    model_name,
                    labels_fingerprint,
                    max_match_score=match["max_match_score"],
                    match_margin=match.get("match_margin"),
                    top_species=match.get("top_species"),
                    label_count=match.get("label_count"),
                    score_kind=match.get("score_kind"),
                )
        except Exception:
            # broad: diagnostic data must never fail a classification run.
            log.warning(
                "Failed to record match score for detection %s", det_id,
                exc_info=True,
            )


def _cached_prediction_taxonomy(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any] | None:
    row = dict(row)
    if not row.get("source_taxon_id") and row.get("labels_fingerprint") != "tol":
        return None  # Legacy custom-label scientific names were inferred.
    result = {rank: row.get("taxonomy_" + rank) for rank in (
        "kingdom", "phylum", "class", "order", "family", "genus",
    )}
    result.update(scientific_name=row.get("scientific_name"), taxon_id=row.get("source_taxon_id"))
    return {key: value for key, value in result.items() if value} or None


def _prediction_taxonomy(
    tax: Taxonomy | None, species: str, supplied: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Enrich source-backed labels by binomial, preserving source provenance."""
    supplied = supplied or {}
    from species_identity import SpeciesResolver
    identity = SpeciesResolver(taxonomy=tax).resolve(
        species, supplied.get("scientific_name"), supplied if supplied.get("taxon_id") else None,
    )
    name = identity.scientific_name or (None if supplied.get("taxon_id") else species)
    hierarchy = tax.get_hierarchy(name) if tax and name else {}
    return {**hierarchy, **supplied}


def _row_match_score(item: RawResult, species: str | None) -> float | None:
    """This frame's raw match score for ``species``, or None if it has none.

    Burst consensus can hand these storage helpers a species that is not this
    frame's own top-1. The frame's raw score belongs to the label the model
    actually picked here, so attaching it to a substituted species would
    record a measurement that was never taken — the alternatives are searched
    instead, and a miss yields None. None means "not recorded" everywhere
    downstream, never "matched poorly".
    """
    if species is not None and species == item.get("prediction"):
        return item.get("raw_score")
    for alt in item.get("alternatives") or []:
        if alt.get("species") == species:
            return alt.get("raw_score")
    return None


def _store_match_prediction(
    db: Database, item: RawResult, model_name: str, labels_fingerprint: str, tax: Taxonomy | None = None,
    species: str | None = None, confidence: float | None = None, taxonomy: Mapping[str, Any] | None = None,
    store_alternatives: bool = True,
) -> None:
    """Persist an already-labeled classifier result as a reusable cache row.

    A taxonomy ``match`` means the photo's XMP already carries this species, so
    it should not re-enter the pending review queue.  The raw classifier output
    still needs a prediction row, though; otherwise the next non-reclassify run
    sees a classifier_runs key with no cached prediction to surface and pays for
    inference again.  Despite the "cache row" framing, ``item`` is this run's
    own inference output, so the scores go in with
    ``from_fresh_inference=True`` and supersede any earlier runtime's.

    ``store_alternatives`` must be False when ``species``/``confidence`` are a
    consensus override (burst groups): the per-frame ``item["alternatives"]``
    are runner-ups for that frame's own top-1, not for the consensus species,
    and an alternative's confidence can exceed the consensus average. Since
    ``get_predictions_for_detection`` orders ``confidence DESC``, caching them
    would let a per-frame alternative outrank the consensus primary and become
    the cached top-1 on later non-reclassify runs.
    """
    species = cast("str", species or item["prediction"])
    refresh_output = item.get("_replace_prediction_outputs", False) and not item.get("_existing")
    retained_species = [species]
    confidence = cast("float", item["confidence"] if confidence is None else confidence)
    tax_hierarchy = _prediction_taxonomy(tax, species, taxonomy or item.get("taxonomy"))
    db.predictions.add(
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
        refresh_output=refresh_output,
        match_score=_row_match_score(item, species),
        from_fresh_inference=True,
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
        # downstream keyword joins already use SQLite ``COLLATE NOCASE``,
        # so a merged
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
            retained_species.append(alt["species"])
            alt_tax = _prediction_taxonomy(tax, alt["species"], alt.get("taxonomy"))
            db.predictions.add(
                detection_id=item["detection_id"],
                species=alt["species"],
                confidence=round(alt["confidence"], 4),
                model=model_name,
                category="match",
                status="alternative",
                taxonomy=alt_tax,
                labels_fingerprint=labels_fingerprint,
                preserve_manual_review=True,
                refresh_output=refresh_output,
                match_score=alt.get("raw_score"),
                from_fresh_inference=True,
            )
    # add_prediction is INSERT-OR-IGNORE: a row cached as non-match on an
    # earlier pass keeps its stale category here. Re-stamp it 'match' so the
    # downgrade in reconcile_match_review_state still fires if this detection
    # later stops being a match.
    db.predictions.reconcile_match_review_state(
        item["detection_id"], model_name, labels_fingerprint,
        species, "match",
    )
    if refresh_output:
        db.predictions.retain_candidates(item["detection_id"], model_name, labels_fingerprint, retained_species)


def _recognized_taxon_keywords(keywords: Iterable[str] | None, tax: Taxonomy | None) -> list[str]:
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


def _categorize_detection_prediction(prediction: str, existing_keywords: Iterable[str], tax: Taxonomy | None) -> str:
    """Categorize one detection prediction against its photo-level keywords.

    Photo keywords are not detection-scoped, so this only describes whether
    the prediction agrees with the photo's species set. The caller separately
    decides whether a photo-level match is specific enough to auto-accept.
    """
    if not tax:
        return "new"
    from compare import categorize

    return categorize(prediction, existing_keywords, tax)


def _can_auto_accept_detection_prediction(
    prediction: str, category: str, existing_keywords: Iterable[str], tax: Taxonomy | None,
) -> bool:
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


def _reconcile_existing_pending_prediction(
    db: Database, item: RawResult, model_name: str, labels_fingerprint: str, category: str,
    group_id: str | None, vote_count: int | None, total_votes: int | None, individual: str | None,
) -> bool:
    """Refresh a reused cached prediction's review and group state in place.

    Returns True when the cached row exists (nothing further to store), False
    when the caller must still write it.
    """
    existing_id = db.predictions.id_for_key(
        item["detection_id"],
        model_name,
        labels_fingerprint,
        item["prediction"],
    )
    db.predictions.reconcile_match_review_state(
        item["detection_id"], model_name, labels_fingerprint,
        item["prediction"], category,
        auto_accept=False,
    )
    if existing_id is None:
        return False
    if group_id is not None:
        db.predictions.update_group_info(
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
        db.predictions.clear_group_info(
            detection_id=item["detection_id"],
            model=model_name,
            labels_fingerprint=labels_fingerprint,
        )
    return True


def _store_pending_detection_prediction(
    db: Database,
    item: RawResult,
    model_name: str,
    labels_fingerprint: str,
    category: str,
    tax: Taxonomy | None = None,
    group_id: str | None = None,
    vote_count: int | None = None,
    total_votes: int | None = None,
    individual: str | None = None,
) -> None:
    tax_hierarchy = _prediction_taxonomy(tax, item["prediction"], item.get("taxonomy"))
    if item.get("_existing") and _reconcile_existing_pending_prediction(
        db, item, model_name, labels_fingerprint, category,
        group_id, vote_count, total_votes, individual,
    ):
        return

    refresh_output = item.get("_replace_prediction_outputs", False) and not item.get("_existing")
    retained_species = [item["prediction"]]
    db.predictions.add(
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
        match_score=_row_match_score(item, item["prediction"]),
        from_fresh_inference=True,
        refresh_output=refresh_output,
    )
    db.predictions.reconcile_match_review_state(
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
        retained_species.append(alt["species"])
        alt_tax = _prediction_taxonomy(tax, alt["species"], alt.get("taxonomy"))
        db.predictions.add(
            detection_id=item["detection_id"],
            species=alt["species"],
            confidence=round(alt["confidence"], 4),
            model=model_name,
            category=category,
            status="alternative",
            taxonomy=alt_tax,
            labels_fingerprint=labels_fingerprint,
            match_score=alt.get("raw_score"),
            from_fresh_inference=True,
            refresh_output=refresh_output,
        )
    if refresh_output:
        db.predictions.retain_candidates(item["detection_id"], model_name, labels_fingerprint, retained_species)


def _xmp_review_category(
    item: RawResult, tax: Taxonomy | None, comparison_name: Callable[[RawResult], str],
    read_keywords: Callable[[str], Iterable[str]],
) -> tuple[str, bool]:
    """Categorize one result against its photo's XMP keywords.

    Returns ``(category, auto_accept)``; without a taxonomy every result is
    ``"new"`` and goes to review.
    """
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
    return category, auto_accept


def _store_burst_group(
    db: Database, group: list[RawResult], gid: str, model_name: str, labels_fingerprint: str,
    tax: Taxonomy | None, identity_for: Callable[[RawResult], SpeciesIdentity],
    consensus_prediction: Callable[[list[dict[str, Any]]], dict[str, Any] | None],
    member_categories: list[tuple[str, bool]],
) -> tuple[int, int]:
    """Store one burst group's predictions under its consensus vote.

    ``member_categories`` is the ``(category, auto_accept)`` pair for each
    member, computed by the caller *before* opening the write transaction so
    the per-member XMP sidecar reads do not hold SQLite's writer lock across
    a slow or disconnected mounted volume.

    Returns ``(stored, already_labeled)``; both are 0 when the group has no
    consensus.
    """
    current = [(item, category) for item, category in zip(group, member_categories, strict=True)
               if detection_store.reviewer_result_is_current(db, item)]
    if not current:
        return 0, 0
    group = [item for item, _ in current]
    member_categories = [category for _, category in current]
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
        return 0, 0

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
    skipped_match = 0
    for item, (category, auto_accept) in zip(group, member_categories, strict=True):
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
    return stored_in_group, skipped_match


def _store_single_prediction(
    db: Database, item: RawResult, model_name: str, labels_fingerprint: str, tax: Taxonomy | None,
    category: str, auto_accept: bool,
) -> tuple[int, int]:
    """Store a result that formed no burst group.

    ``category`` / ``auto_accept`` are computed by the caller before the
    write transaction opens; see ``_store_burst_group`` for why the XMP
    sidecar read stays out of the transaction.

    Returns ``(stored, already_labeled)``: ``(0, 1)`` when it auto-accepted
    as a match, ``(1, 0)`` when it went to review.
    """
    if not detection_store.reviewer_result_is_current(db, item):
        return 0, 0
    if auto_accept:
        _store_match_prediction(
            db, item, model_name, labels_fingerprint, tax,
        )
        return 0, 1

    _store_pending_detection_prediction(
        db, item, model_name, labels_fingerprint, category, tax,
    )
    return 1, 0


def _release_stored_classifier_runs(
    db: Database, raw_results: list[RawResult], model_name: str, labels_fingerprint: str,
) -> None:
    """Mark this run's freshly stored classifier_runs reusable, in one unit.

    A flush only reserves a run. It becomes reusable once its predictions
    were actually stored. Cancellation before this point leaves an explicit
    cache miss instead of an empty 'legacy' cache hit.
    """
    with db.transaction(immediate=any(r.get("_reviewer_box") is not None for r in raw_results)):
        for item in raw_results:
            if not detection_store.reviewer_result_is_current(db, item):
                continue
            did = item.get("detection_id")
            if did is not None and not item.get("_existing"):
                db.model_runs.release_incomplete_classifier_run(
                    did, model_name, labels_fingerprint,
                )


def _store_grouped_predictions(
    raw_results: list[RawResult], job_id: str, model_name: str, grouping_window: int,
    similarity_threshold: float, tax: Taxonomy | None, db: Database,
    labels_fingerprint: str = "legacy",
) -> dict[str, int]:
    """Publish only results still bound to the reviewer region that was read."""
    with detection_store.guard_reviewer_results(db, raw_results):
        return _store_current_predictions(
            raw_results, job_id, model_name, grouping_window,
            similarity_threshold, tax, db, labels_fingerprint,
        )


def _store_current_predictions(
    raw_results: list[RawResult], job_id: str, model_name: str, grouping_window: int,
    similarity_threshold: float, tax: Taxonomy | None, db: Database,
    labels_fingerprint: str = "legacy",
) -> dict[str, int]:
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

    # Both standalone and Process jobs reach this storage boundary. Fresh
    # inference replaces the old recipe's outputs while cached rows survive.
    for item in raw_results:
        if not item.get("_existing"):
            item["_replace_prediction_outputs"] = True

    # Before grouping: consensus can replace an item's species with the burst's
    # winner, and the already-labeled check can drop items entirely, but the
    # match score describes what the classifier saw on this frame. Record it
    # while it still refers to this frame's own run.
    _record_match_scores(db, raw_results, model_name, labels_fingerprint)

    def identity_for(item: RawResult) -> SpeciesIdentity:
        supplied = item.get("taxonomy") or {}
        source = supplied if supplied.get("taxon_id") else None
        return resolver.resolve(item["prediction"], supplied.get("scientific_name"), source)

    def comparison_name(item: RawResult) -> str:
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
        # XMP sidecar reads happen outside the batch transaction so a slow
        # mount cannot block other catalog writers under the writer lock.
        member_categories = [
            _xmp_review_category(item, tax, comparison_name, read_keywords)
            for item in group
        ]
        if len(group) == 1:
            with db.transaction(immediate=group[0].get("_reviewer_box") is not None):  # one photo's predictions land together
                stored, matched = _store_single_prediction(
                    db, group[0], model_name, labels_fingerprint, tax,
                    *member_categories[0],
                )
        else:
            group_count += 1
            group_namespace = hashlib.sha256(
                json.dumps([job_id, model_name, labels_fingerprint]).encode()
            ).hexdigest()[:16]
            gid = f"g{group_namespace}-{group_count:04d}"
            with db.transaction(immediate=any(r.get("_reviewer_box") is not None for r in group)):  # one burst group, consensus + members
                stored, matched = _store_burst_group(
                    db, group, gid, model_name, labels_fingerprint, tax,
                    identity_for, consensus_prediction, member_categories,
                )
        predictions_stored += stored
        skipped_match += matched

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

    _release_stored_classifier_runs(db, raw_results, model_name, labels_fingerprint)
    return {
        "predictions_stored": predictions_stored,
        "burst_groups": group_count,
        "already_labeled": skipped_match,
    }
