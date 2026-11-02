"""The prediction-decision writers: every write of a prediction's review decision.

A review decision is a ``prediction_review`` row for one workspace: accepted,
rejected, reviewed, an ungroup, or the accept that also tags the photo with
the species. Each writer here is the single entry point for its kind of
decision, so ``test_route_contract`` finds every route that can record one
by these names (``_PREDICTION_MUTATORS``) and requires it to hold the
prediction-decision lock (``services.prediction_decisions``). Outside the
data layer nothing else calls the repository writers behind them
(``PredictionRepository.update_status`` / ``update_status_by_photo`` /
``ungroup`` / ``set_review_status``,
``KeywordProvenanceRepository.accept_prediction``); ``test_route_contract``
enforces that too.

None of these commits: the caller's unit of work (the decision lock) owns
the transaction.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from species_identity import SpeciesResolver

if TYPE_CHECKING:
    from db import Database


def update_prediction_status(db: Database, prediction_id: int, status: str) -> None:
    """Update per-workspace review status for a prediction (no commit).

    Review state lives in ``prediction_review`` keyed by
    (prediction_id, workspace_id); the repository upserts rather than
    UPDATEs so the "first review in a fresh workspace" path still writes a
    row.
    """
    db.predictions.update_status(prediction_id, status)


def update_predictions_status_by_photo(db: Database, photo_id: int, status: str) -> None:
    """Upsert review status for every prediction of a photo in the active workspace.

    Review state is workspace-scoped (``prediction_review``); detections
    and predictions are global. The repository enumerates the prediction
    ids via the detections join and upserts each review row.

    Does not commit: the caller holds the prediction decision lock
    (``services.prediction_decisions``) and owns the transaction.

    Deliberately unconditional. The stale-overwrite this used to enable — a
    group apply flipping an ``accepted`` row to ``rejected`` while the
    keyword the accept added stays on the photo — is guarded one level up,
    in ``api_prediction_group_apply``, against the statuses the burst modal
    displayed (``_stale_group_apply_photos``). A ``WHERE status NOT IN
    DECIDED_PREDICTION_STATUSES`` clause on the upsert was tried first and
    removed: it cannot tell a stale payload from the user re-opening an
    applied burst and changing the split, which is a reachable flow
    (Review's default tab is "All", and every grouped card there renders
    "Review Burst Group"), so it silently froze the statuses while the flag
    and keyword writes above it — which run before this call and outside
    the lock — went ahead anyway. Guarding here is both too strict and too
    late.
    """
    db.predictions.update_status_by_photo(photo_id, status)


def ungroup_prediction(db: Database, prediction_id: int) -> None:
    """Remove a prediction from its group in the active workspace (no commit).

    ``group_id`` lives in ``prediction_review``; this only clears the
    review row for the current workspace.
    """
    db.predictions.ungroup(prediction_id)


def set_review_status(
    db: Database,
    prediction_id: int,
    workspace_id: int,
    status: str,
    individual: str | None = None,
    group_id: str | None = None,
) -> None:
    """Write one workspace's review row for a prediction as given (no commit)."""
    db.predictions.set_review_status(
        prediction_id, workspace_id, status, individual=individual, group_id=group_id,
    )


def accept_prediction(
    db: Database,
    prediction_id: int,
    replace_species: bool = False,
    photo_ids: Collection[int] | None = None,
    prediction_ids: Collection[int] | None = None,
    species_taxonomy: Any = ...,
) -> dict[str, Any] | None:
    """Accept a prediction: mark as accepted and add species keyword.

    If the prediction belongs to a group, derives the consensus species
    from the individual votes and applies that to all photos.

    When ``replace_species`` is True, every photo that receives the new
    keyword first has its existing species/taxonomy keywords removed, so
    grouped photos are replaced consistently rather than accumulating both
    the old and new species tags. Each entry in the returned ``affected``
    list carries the ``old_species`` names that were stripped from that
    photo (empty when ``replace_species`` is False).

    When ``photo_ids`` is provided for a grouped prediction, only matching
    group members are tagged and marked accepted. This lets callers apply a
    grouped accept to a filtered subset without changing hidden photos.

    ``prediction_ids`` is the stricter form of the same limit, for callers
    that already know the exact prediction rows they are acting on: the
    grouped accept touches only those rows. Prefer it over ``photo_ids``
    whenever the caller has a submitted id list. A photo is not a unique
    key for a prediction — one photo can carry several rows in the same
    burst group (one per classifier model, or per detection) — so a
    photo-id limit lets a grouped accept reach a row on an allowed photo
    that the caller never submitted. ``photo_ids`` remains for callers
    whose intent really is "these photos" (highlight confirm, accept
    subject), where the row set is chosen by this function.

    Independently of either limit, group expansion only ever *discovers*
    undecided rows: a group member already ``accepted`` or ``rejected`` is
    left alone unless the caller named it (as the entry row, or in
    ``prediction_ids``). Accepting one burst member must not resurrect a
    sibling the user rejected in Review, nor re-flip a long-accepted one
    into a history item whose "previous" status never happened.

    Both limits are settled before the first write, so a call whose scope
    excludes every candidate row is a true no-op: no keyword row is
    created, no sibling alternative is rejected, no status is flipped. It
    returns ``accepted_prediction_ids: []`` with ``keyword_id`` set to the
    existing keyword for the resolved species (``None`` when no such
    keyword exists yet).

    The returned ``accepted_prediction_ids`` lists every prediction row
    this call marked accepted, so a caller looping over a submitted batch
    can skip rows a previous grouped accept already covered instead of
    re-accepting them into duplicate history items.

    ``species_key`` is the resolved consensus identity, independent of
    the particular keyword alias used to tag the photos. Batch callers
    compare this key and record each result's actual ``keyword_id`` for
    undo/redo rather than requiring equivalent aliases to share an ID.

    ``species_taxonomy`` is the loaded taxonomy (``load_local_taxonomy()``)
    a ``replace_species`` accept uses to keep keywords a neighbouring
    subject's prediction supports as broader/same/narrower. Replacement
    requires an explicit choice: pass the loaded taxonomy, or pass None
    to choose exact-name protection when taxonomy is unavailable. Omitting
    it raises ValueError before any writes. The caller loads it, so the
    data layer never reads the taxonomy file.

    Does not commit: the caller's unit of work (the prediction-decision
    lock) makes the accept atomic.
    """
    return db.keyword_provenance.accept_prediction(
        prediction_id, replace_species=replace_species, photo_ids=photo_ids,
        prediction_ids=prediction_ids,
        species_taxonomy=species_taxonomy,
    )


def accept_subject_species(db: Database, prediction_id: int) -> dict[str, Any] | None:
    """Accept agreeing model predictions for one detected subject.

    Compare uses this for an additional-species suggestion: the species
    keyword is added once to the photo, the existing species keywords are
    preserved, and every current-model prediction that names the same
    species on the same detection is resolved together. Grouped
    predictions are explicitly limited to this photo so accepting a
    subject in Compare cannot silently tag the rest of a burst.

    Does not commit: ``api_accept_subject_species`` bundles the accept
    with its own writes (edit history, precondition checks) in the
    prediction-decision unit of work.
    """
    target = db.predictions.subject_accept_target(prediction_id)
    if target is None:
        return None

    resolver = SpeciesResolver(db=db)
    target_key = resolver.consensus(db.predictions.get_rows_by_ids([prediction_id])[0]).key

    agreeing = db.predictions.pending_current_on_detection(target["detection_id"])

    identities = {row['id']: resolver.consensus(row).key
                  for row in db.predictions.get_rows_by_ids([p['id'] for p in agreeing])}
    agreeing = [row for row in agreeing if identities[row['id']] == target_key]

    accepted_ids = []
    affected = []
    result = None
    for row in agreeing:
        accepted = accept_prediction(
            db,
            row["id"],
            photo_ids=[target["photo_id"]],
            prediction_ids=[row["id"]],
        )
        # A row whose grouped scope excluded this photo accepts
        # nothing, so it must not be reported as an accepted id (nor
        # supply the returned keyword, which is None when the species
        # keyword does not exist yet).
        if accepted and accepted["accepted_prediction_ids"]:
            result = accepted
            accepted_ids.append(row["id"])
            # Equivalent legacy labels can return different keyword
            # IDs. History must retain the ID each accept touched.
            affected.extend({**a, "keyword_id": accepted["keyword_id"]}
                            for a in accepted["affected"])

    if result is None:
        return None
    return {
        "species": result["species"],
        "keyword_id": result["keyword_id"],
        "photo_id": target["photo_id"],
        "prediction_ids": accepted_ids,
        "affected": affected,
    }
