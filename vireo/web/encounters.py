"""Encounter species confirmation from the pipeline review page.

``/api/encounters/species`` confirms (replaces, adds or removes) the species
for every photo in a cached encounter or a single burst: it tags the photos,
queues the sidecar keyword changes, rewrites the pipeline results cache and
records a grouping-history edit. It takes the workspace regroup lock and the
shared prediction-decision lock through ``services.prediction_decisions``.

``_confirm_encounter_species`` validates the request and runs the edit in one
transaction; ``_SpeciesConfirmation`` carries one confirmation's state, with
one method per phase.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, cast

from config import config_value
from flask import Blueprint, jsonify, request
from keyword_normalization import keyword_match_key, normalize_keyword_display
from pipeline_results import auto_detach_burst_for_species
from services import prediction_decisions
from services.pending_changes import queue_keyword_add, queue_keyword_remove

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask import Response
    from flask.typing import ResponseReturnValue
    from services.service_types import ErrorResponse, JsonDict, JsonError
    from web.web_types import GetDb

log = logging.getLogger(__name__)


def create_encounters_blueprint(get_db: GetDb, json_error: JsonError, db_path: str) -> Blueprint:
    """Build the encounter species-confirmation blueprint.

    ``db_path`` locates the pipeline results cache (it lives next to the
    database).
    """
    blueprint = Blueprint("encounters", __name__)

    @blueprint.route("/api/encounters/species", methods=["POST"])
    def api_encounter_species() -> ResponseReturnValue:
        """Confirm species for all photos in an encounter or a single burst.

        Expects JSON: {"species": "Blue Jay", "photo_ids": [1, 2, 3],
                       "burst_index": <int|null>,
                       "add": <bool>, "remove": <bool>,
                       "previous_species": <str|null>}

        Creates the species keyword, tags photos, and queues a sidecar add.
        If the encounter (or burst) was previously confirmed as a different
        species, also untags that species and queues a sidecar remove — or
        cancels the still-pending add if it hadn't synced yet — so the XMP
        doesn't accumulate stale species keywords.

        A burst can legitimately hold two species (two subjects), so the
        confirmation is a *set* edit with three modes:

        * replace (default): ``species`` swaps out ``previous_species`` —
          the burst's primary confirmed species unless the client names
          another entry of the current list.
        * ``add``: ``species`` joins the current list; nothing is untagged
          and the burst is never auto-detached.
        * ``remove``: ``species`` is untagged from the submitted photos and
          dropped from the list; no keyword is added.
        """
        from pipeline_locks import acquire_workspace_regroup

        db = get_db()
        cache_restore = _CacheRestore()
        with acquire_workspace_regroup(db.require_workspace_id()), cache_restore:
            return prediction_decisions.under_prediction_decision_lock(
                db, lambda: _confirm_encounter_species(db, cache_restore),
                json_error=json_error,
            )

    def _confirm_encounter_species(db: Database, cache_restore: _CacheRestore) -> ErrorResponse | Response:
        """Confirm species while holding the grouping and database writer locks."""
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("Expected a JSON object")
        if body.get("response_mode", "full") not in ("full", "compact"):
            return json_error("response_mode must be full or compact")
        species = body.get("species", "")
        if not isinstance(species, str):
            return json_error("species must be a string")
        species = species.strip()
        photo_ids = body.get("photo_ids", [])
        burst_index = body.get("burst_index")
        add_mode = bool(body.get("add"))
        remove_mode = bool(body.get("remove"))
        requested_previous = body.get("previous_species")
        if add_mode and remove_mode:
            return json_error("add and remove are mutually exclusive")
        if requested_previous is not None and (add_mode or remove_mode):
            return json_error("previous_species only applies to a replace")

        # Normalize up front: this route compares the requested species
        # against stored keyword rows and previous_species cache values
        # below, and add_keyword would normalize on insert anyway — keep one
        # spelling throughout. Rejects quote-only input as empty.
        species = normalize_keyword_display(species)
        if not species:
            return json_error("species is required")
        if not photo_ids:
            return json_error("photo_ids is required")

        error = _photo_ids_error(db, photo_ids, json_error)
        if error is not None:
            return error
        from pipeline import load_results_raw

        confirmation = _SpeciesConfirmation(
            db, json_error, species, photo_ids, burst_index,
            add_mode=add_mode,
            remove_mode=remove_mode,
            cache_dir=os.path.dirname(db_path),
        )
        error = confirmation.read_provenance(body)
        if error is not None:
            return error
        confirmation.load_cache(load_results_raw)
        error = confirmation.burst_error()
        if error is not None:
            return error
        confirmation.read_current_species()
        if body.get("preview") or "preview_token" in body:
            # A mixed encounter may have no common cached species. The reviewer
            # may still explicitly replace a species carried by selected frames.
            actual = db.photo_species.get_species_for_photos(photo_ids)
            for names in actual.values():
                for name in names:
                    if name not in confirmation.current_species_list:
                        confirmation.current_species_list.append(name)
        error = confirmation.resolve_previous_species(requested_previous)
        if error is not None:
            return error

        confirmation.resolve_old_species_rows()
        error = _review_preview(db, confirmation, body, json_error)
        if error is not None:
            return error
        low_confidence_photo_ids = _low_confidence_photo_ids(db, photo_ids)

        return _commit_species_edit(
            db, confirmation.before_cached, confirmation.cache_dir,
            lambda: _apply_confirmation(confirmation, low_confidence_photo_ids, body.get("response_mode", "full")),
            cache_restore,
        )

    @blueprint.route("/api/encounters/species/bulk", methods=["POST"])
    def api_encounter_species_bulk() -> ResponseReturnValue:
        """Add distinct species selections atomically, with one compact result."""
        from pipeline_locks import acquire_workspace_regroup
        db = get_db()
        cache_restore = _CacheRestore()
        with acquire_workspace_regroup(db.require_workspace_id()), cache_restore:
            return prediction_decisions.under_prediction_decision_lock(
                db, lambda: _bulk_species(db, request.get_json(silent=True), db_path, json_error, cache_restore),
                json_error=json_error,
            )

    return blueprint



class _ErrorFor(Protocol):
    """``json_error`` or a wrapper naming the bulk assignment at fault."""

    def __call__(self, msg: str, status: int = 400, /) -> ErrorResponse: ...


def _apply_confirmation(
    confirmation: _SpeciesConfirmation, low_confidence_photo_ids: list[int], response_mode: str,
) -> tuple[JsonDict, JsonDict | None]:
    """Stage an individual confirmation without committing or saving the cache."""
    ws_id = confirmation.db.require_workspace_id()
    confirmation.resolve_target_keyword()
    confirmation.untag_previous_species(ws_id)
    confirmation.tag_new_species(ws_id)
    confirmation.record_photo_edit()
    confirmation.compute_cache_state()
    confirmation.record_cache_only_confirmation()
    confirmation.apply_cache_mutation()
    return confirmation.response(low_confidence_photo_ids, response_mode), confirmation.cached


class _CacheRestore:
    """Puts the pipeline cache back if the decision it was saved in fails.

    The confirmation writes the catalog and the Process results cache
    together: the cache file is saved inside the decision's unit of work,
    before the unit commits. Used as a context manager around the whole
    decision, so a failure anywhere after the save, including the unit's own
    commit, restores the cache the catalog still matches.
    """

    def __init__(self) -> None:
        self.restore: Callable[[], object] | None = None

    def __enter__(self) -> _CacheRestore:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *_exc: object) -> None:
        if exc_type is None or self.restore is None:
            return
        try:
            self.restore()
        except (OSError, TypeError, ValueError):
            log.exception("Failed to restore pipeline cache after species confirmation failed")


def _commit_species_edit(
    db: Database, before_cached: JsonDict | None, cache_dir: str,
    work: Callable[[], tuple[JsonDict, JsonDict | None]], cache_restore: _CacheRestore,
) -> Response:
    """Write catalog and cache in the decision's unit; ``cache_restore`` undoes the cache."""
    from pipeline import save_results_raw
    result, cached = work()
    if cached and cached != before_cached:
        cache_restore.restore = lambda: save_results_raw(before_cached, cache_dir, db.active_workspace_id)
        save_results_raw(cached, cache_dir, db.active_workspace_id)
    if cached:
        result["summary"] = cached.get("summary", {})
    db.after_commit(db._prune_edit_history)
    return jsonify(result)


def _bulk_species(
    db: Database, body: object, db_path: str, json_error: JsonError, cache_restore: _CacheRestore,
) -> ErrorResponse | Response:
    """Validate every target before starting an all-or-nothing additive batch."""
    if not isinstance(body, dict) or set(body) - {"assignments", "response_mode"}:
        return json_error("Expected assignments and optional response_mode")
    if body.get("response_mode", "compact") != "compact":
        return json_error("Bulk assignments require response_mode compact")
    assignments = body.get("assignments")
    if not isinstance(assignments, list) or not assignments or len(assignments) > 100:
        return json_error("assignments must contain between 1 and 100 selections")
    validated = []
    for i, assignment in enumerate(assignments):
        def invalid(message: str, status: int = 400, index: int = i) -> ErrorResponse:
            return json_error(f"Assignment {index}: {message}; no assignments applied", status)
        if not isinstance(assignment, dict) or set(assignment) - {"species", "photo_ids", "detection_ids"}:
            return invalid("Expected species, photo_ids and optional detection_ids (additive only)")
        species = assignment.get("species")
        if not isinstance(species, str) or not normalize_keyword_display(species):
            return invalid("species must be a nonempty string")
        ids = assignment.get("photo_ids")
        error = _photo_ids_error(db, ids, invalid)
        if error is not None:
            return error
        error = _detection_ids_error(db, assignment, invalid)
        if error is not None:
            return error
        # _photo_ids_error accepted ``ids`` as a list of unique ints.
        validated.append({**assignment, "species": normalize_keyword_display(species),
                          "low_confidence_photo_ids": _low_confidence_photo_ids(db, cast("list[int]", ids))})
    from pipeline import load_results_raw
    from services.bulk_species_assignments import apply_assignments
    cache_dir = os.path.dirname(db_path)
    cached = load_results_raw(cache_dir, db.active_workspace_id)
    before = copy.deepcopy(cached)
    return _commit_species_edit(
        db, before, cache_dir, lambda: (apply_assignments(db, cached, validated, before=before), cached),
        cache_restore,
    )


def _detection_ids_error(db: Database, assignment: JsonDict, json_error: _ErrorFor) -> ErrorResponse | None:
    """Subject selections must belong to exactly the submitted photos."""
    if "detection_ids" not in assignment:
        return None
    ids = assignment["detection_ids"]
    if (not isinstance(ids, list) or not ids or any(type(did) is not int for did in ids)
            or len(ids) != len(set(ids))):
        return json_error("detection_ids must be a nonempty list of unique integers")
    selected = set(ids)
    owners = db.detections.get_ids_for_photos(assignment["photo_ids"])
    if (not selected <= set().union(*owners.values())
            or any(not selected.intersection(owners.get(pid, set())) for pid in assignment["photo_ids"])):
        return json_error("detection_ids must belong to exactly the selected photo_ids")
    return None


def _review_preview(
    db: Database, confirmation: _SpeciesConfirmation, body: JsonDict, json_error: JsonError,
) -> ErrorResponse | Response | None:
    """Preview and revalidate a deliberate, exact subset before any writes."""
    if not body.get("preview") and "preview_token" not in body:
        if "group_changes" in body:
            return json_error("Combined Group Review changes require a species preview", 409)
        return None  # Existing API clients retain their established contract.
    from services.encounter_confirmation import preview

    source = body.get("source_photo_ids", confirmation.photo_ids)
    if (not isinstance(source, list) or not source
            or any(type(pid) is not int for pid in source)
            or len(set(source)) != len(source)
            or not set(confirmation.photo_ids) <= set(source)):
        return json_error("source_photo_ids must contain the selected photo_ids exactly once")
    error = _photo_ids_error(db, source, json_error)
    if error is not None:
        return error
    enc = confirmation.target_enc
    if enc is None or not set(source) <= set(enc["photo_ids"]):
        return json_error("Photo membership changed. Reload the review and preview again.", 409)
    if confirmation.burst_index is not None:
        burst = enc["bursts"][confirmation.burst_index]
        if not set(source) <= set(burst["photo_ids"]):
            return json_error("Burst membership changed. Preview again.", 409)
    if body.get("split") and set(confirmation.photo_ids) == set(enc["photo_ids"]):
        return json_error("Select fewer than all encounter photos to separate a transition")
    if confirmation.is_replacement and confirmation.old_kid_row is not None:
        key = keyword_match_key(confirmation._replaced_species)
        confirmation._collect_old_rows(key, _HomonymConflicts(db, key))
    from services.review_group_changes import InvalidGroupChanges, validate_group_changes
    try:
        # ``enc`` came from the loaded cache, so the cache is not None.
        confirmation.group_plan = validate_group_changes(db, cast("JsonDict", confirmation.cached), enc, body)
    except InvalidGroupChanges as exc:
        return json_error(str(exc), 409)
    result = preview(db, confirmation, body)
    if body.get("preview"):
        return jsonify(result)
    if body.get("preview_token") != result["preview_token"]:
        return json_error("Photos, species or review choices changed. Preview again before applying.", 409)
    # The removal loop collects its rows again inside the write phase.
    confirmation.old_rows_by_photo = {}
    confirmation.preview_scope = True
    confirmation.split = bool(body.get("split"))
    return None


def _photo_ids_error(db: Database, photo_ids: object, json_error: _ErrorFor) -> ErrorResponse | None:
    """The error response for unknown or out-of-workspace photo_ids, else None."""
    if (not isinstance(photo_ids, list) or not photo_ids
            or any(type(pid) is not int for pid in photo_ids)
            or len(set(photo_ids)) != len(photo_ids)):
        return json_error("photo_ids must be a nonempty list of unique integers")
    # Validate all photo_ids exist before mutating.
    found_ids = db.photos.existing_ids(photo_ids)
    missing = [pid for pid in photo_ids if pid not in found_ids]
    if missing:
        return json_error(f"Unknown photo_ids: {missing}")
    # Photos are global; a confirmation tags keywords and queues sidecar
    # writes under the active workspace, so it may only touch photos
    # that workspace can see.
    visible_ids = set(db.photo_visibility.visible_photo_ids(photo_ids))
    foreign = [pid for pid in photo_ids if pid not in visible_ids]
    if foreign:
        return json_error(
            f"photo_ids not in the active workspace: {foreign}", 403
        )
    return None


def _low_confidence_photo_ids(db: Database, photo_ids: list[int]) -> list[int]:
    """Photos whose only detections are below detector_confidence.

    Surface photos whose only detections are below the workspace's
    detector_confidence threshold, but do not drop them. This endpoint
    represents an explicit user confirmation, and that assertion should
    win over a weak detector box. Fully automatic labeling paths should
    do their own filtering before calling lower-level tag helpers.
    """
    import config as cfg
    effective_cfg = db.get_effective_config(cfg.load())
    det_conf_threshold = config_value(effective_cfg, "detector_confidence")
    det_rows = db.detections.confidence_summary(photo_ids)
    return [
        r["photo_id"] for r in det_rows
        if r["n"] > 0 and (r["max_conf"] or 0) < det_conf_threshold
    ]


class _HomonymConflicts:
    """Whether a previous-species row has a linked homonym elsewhere.

    When the previous species is linked to a taxon and another
    taxonomy row anywhere in the catalog shares the same
    normalized name but points at a different taxon (e.g.
    legacy ``Robin`` alongside taxonomy ``robin``), an
    unlinked NULL-taxon row could belong to either species.
    Treating it as the old species would queue a legacy
    homonym tag for removal. Mirror the guard in
    get_photos_with_equivalent_species.

    The same guard applies when the *previous species* is
    unlinked: any distinct linked same-key row is a different
    species, and folding it in would let encounter replacement
    delete a taxonomy species from the photo.

    Cache the check per (taxon_id, kid_id) — hierarchy fallback
    can resolve different taxa across submitted photos, and
    each unique taxon gets its own homonym conflict answer.
    """

    def __init__(self, db: Database, old_target_key: str) -> None:
        self.db = db
        self.old_target_key = old_target_key
        self.cache: dict[tuple[int | None, int | None], bool] = {}

    def check(self, target_taxon_id: int | None, target_kid_id: int) -> bool:
        cache_key = (
            target_taxon_id,
            None if target_taxon_id is not None else target_kid_id,
        )
        if cache_key in self.cache:
            return self.cache[cache_key]
        conflict = False
        for name in self.db.keyword_taxa.other_linked_species_names(
            target_taxon_id, target_kid_id,
        ):
            if keyword_match_key(name) == self.old_target_key:
                conflict = True
                break
        self.cache[cache_key] = conflict
        return conflict


def _is_previous_species_row(
    row: Row, photo_old: Row, old_target_key: str, homonym_conflicts: _HomonymConflicts,
) -> bool:
    """Whether an attached species row is the photo's resolved old species."""
    eff_taxon_id = photo_old["taxon_id"]
    eff_kid_id = photo_old["id"]
    eff_homonym_conflict = homonym_conflicts.check(eff_taxon_id, eff_kid_id)
    if eff_taxon_id is None:
        if eff_homonym_conflict:
            # Unlinked previous species with a linked
            # same-key homonym in the catalog: only the
            # exact resolved old row is safe to remove.
            return row["id"] == eff_kid_id
        return keyword_match_key(row["name"]) == old_target_key
    return (
        row["taxon_id"] == eff_taxon_id
    ) or (
        not eff_homonym_conflict
        and row["taxon_id"] is None
        and keyword_match_key(row["name"]) == old_target_key
    )


class _SpeciesConfirmation:
    """One species confirmation over a cached encounter or a single burst.

    Holds the request, the pipeline results cache it edits, and what each
    phase resolved for the next. Methods run in the order
    ``_confirm_encounter_species`` calls them.
    """

    def __init__(
        self, db: Database, json_error: JsonError, species: str, photo_ids: list[int], burst_index: int | None, *,
        add_mode: bool, remove_mode: bool, cache_dir: str,
    ) -> None:
        self.db = db
        self.json_error = json_error
        self.species = species
        self.photo_ids = photo_ids
        self.burst_index = burst_index
        self.add_mode = add_mode
        self.remove_mode = remove_mode
        self.cache_dir = cache_dir

        self.group_plan: JsonDict | None = None
        self.group_photo_states: dict[int, JsonDict] | None = None
        self.preview_scope = False
        self.split = False
        self.cached: JsonDict | None = None
        self.before_cached: JsonDict | None = None
        self.target_enc: JsonDict | None = None
        self.target_enc_idx: int | None = None
        self.current_species_list: list[str] = []
        self.previous_species: str | None = None

        self.is_replacement = False
        self.old_kid_row: Row | None = None
        # ``per_photo_old_row`` maps each submitted photo to the effective
        # old-species keyword row the removal loop should match against.
        # For root-lookup (parent_id IS NULL) hits every photo shares the
        # same row; for the hierarchy-leaf fallback, catalogs can hold the
        # same alias under different taxa across submitted photos (a
        # legitimate homonym; duplicate repair deliberately preserves such
        # rows), so each photo gets its own resolved row. A single
        # ``best_taxon`` used to select one row for the whole batch would
        # leave every photo on a different taxon with its old leaf still
        # attached — the removal loop matches by ``taxon_id``.
        self.per_photo_old_row: dict[int, Row] = {}

        self.kid: int | None = None
        self.already_has_new: set[int] = set()
        self.newly_tagged: list[int] = []
        self.old_rows_by_photo: dict[int, list[Row]] = {}
        self.had_old: set[int] = set()
        self.photo_edit_id: int | None = None
        self.provenance: JsonDict | None = None
        self.preserve_existing_provenance = False
        self.preserve_keyword_metadata = False
        self.preserved_keyword_id: int | None = None
        self.attribution_only: list[int] = []
        self.undo_edit_id: int | None = None

        self.new_species_list: list[str] = []
        # Set by compute_cache_state / _read_frame_species before any reader.
        self.cache_description = ""
        self.actual_by_photo: dict[int, list[str]] = {}
        self.new_burst_override: JsonDict | None = None
        self.will_auto_detach = False
        self.new_encounter_state: JsonDict | None = None

    @property
    def _replaced_species(self) -> str:
        """``previous_species``, which is set whenever ``is_replacement`` holds."""
        return cast(str, self.previous_species)

    def read_provenance(self, body: JsonDict) -> ErrorResponse | None:
        error = self.read_keyword_scope(body)
        if error is not None:
            return error
        preserve = body.get('preserve_existing_provenance', False)
        if type(preserve) is not bool:
            return self.json_error('preserve_existing_provenance must be a boolean')
        self.preserve_existing_provenance = preserve
        if 'provenance' not in body:
            return None
        from services.species_provenance import validate_provenance
        try:
            self.provenance = validate_provenance(self.db, self.photo_ids, body['provenance'])
        except ValueError as exc:
            return self.json_error(str(exc))
        return None

    def read_keyword_scope(self, body: JsonDict) -> ErrorResponse | None:
        """Reuse a species keyword without promoting or rebinding shared rows.

        The route holds BEGIN IMMEDIATE from validation through application, so
        the resolved row cannot change between this read and the association edit.
        """
        preserve = body.get('preserve_keyword_metadata', False)
        if type(preserve) is not bool:
            return self.json_error('preserve_keyword_metadata must be a boolean')
        self.preserve_keyword_metadata = preserve
        if not preserve or self.remove_mode:
            return None
        target = self.db.keywords.species_confirmation_target(self.species)
        keyword = target['keyword']
        if keyword is None:
            return None
        rank = next((t['rank'] for t in target['taxa'] if t['id'] == keyword['taxon_id']), None)
        if (keyword['type'] != 'taxonomy' or rank not in (None, 'species')
                or not (keyword['is_species'] or rank == 'species')):
            return self.json_error(
                'The existing keyword needs explicit review of its type or taxon in keyword management '
                'before a scoped species assignment; no photos were changed', 409,
            )
        self.preserved_keyword_id = keyword['id']
        return None

    # -- request against the cache ----------------------------------

    def load_cache(self, load_results_raw: Callable[[str, int | None], JsonDict | None]) -> None:
        """Load the pipeline results and find the encounter holding the photos."""
        self.cached = load_results_raw(
            self.cache_dir, self.db.active_workspace_id,
        )
        self.before_cached = copy.deepcopy(self.cached)
        if self.cached:
            photo_id_set = set(self.photo_ids)
            for enc_idx, enc in enumerate(self.cached.get("encounters", [])):
                enc_ids = set(enc.get("photo_ids", []))
                if not photo_id_set.issubset(enc_ids):
                    continue
                enc["bursts"] = [{"photo_ids": burst} if isinstance(burst, list) else burst
                                 for burst in enc.get("bursts", [])]
                self.target_enc = enc
                self.target_enc_idx = enc_idx
                break

    def burst_error(self) -> ErrorResponse | None:
        """Reject a burst_index that does not hold the submitted photos.

        If this is a burst-scoped request, the burst must actually exist in
        the cached encounter AND the submitted photo_ids must be a subset of
        that burst's photos. Otherwise a stale client (e.g. one that still
        holds a burst index from before a regrouping) could retag photos
        that don't belong to this burst while the later cache update touches
        the wrong override.
        """
        burst_index = self.burst_index
        if burst_index is None:
            return None
        if type(burst_index) is not int:
            return self.json_error("burst_index must be an integer")
        target_enc = self.target_enc
        bursts = target_enc.get("bursts") if target_enc else None
        if not bursts or not (0 <= burst_index < len(bursts)):
            return self.json_error(
                f"Unknown burst_index {burst_index} for submitted photos",
            )
        burst_photo_ids = set(bursts[burst_index].get("photo_ids", []))
        if not set(self.photo_ids).issubset(burst_photo_ids):
            return self.json_error(
                f"photo_ids are not members of bursts[{burst_index}]",
            )
        return None

    def read_current_species(self) -> None:
        """Read the burst's (or encounter's) confirmed species list."""
        from pipeline_results import (
            burst_species_list,
            encounter_confirmed_species_list,
        )

        current_species_list = []
        target_enc = self.target_enc
        if target_enc is not None:
            if self.burst_index is not None:
                # A burst override wins when recorded; otherwise the burst
                # inherits the encounter's confirmed species, which is what
                # those photos were actually tagged with.
                current_species_list = burst_species_list(
                    target_enc, target_enc["bursts"][self.burst_index],
                )
            else:
                current_species_list = encounter_confirmed_species_list(
                    target_enc,
                )

        # The cached list comes from the pipeline cache on disk, which the
        # v5 DB migration does not touch — a pre-normalization cache can
        # still carry a quoted spelling like `‘Apapane`. Normalize before
        # comparing/looking up against stored keyword rows, which are always
        # clean.
        self.current_species_list = [
            name for name in (
                normalize_keyword_display(s) for s in current_species_list
            ) if name
        ]

    def _confirmed_species_matching(self, name: str) -> str | None:
        """The current list's entry with ``name``'s match key, else None."""
        requested_key = keyword_match_key(name)
        return next(
            (
                s for s in self.current_species_list
                if keyword_match_key(s) == requested_key
            ),
            None,
        )

    def resolve_previous_species(self, requested_previous: str | None) -> ErrorResponse | None:
        """Pick the species this edit replaces or removes, per mode."""
        if self.remove_mode:
            # Only a currently confirmed species can be removed; otherwise a
            # stale client could strip an unrelated keyword from the photos
            # while the cache list (which has nothing to drop) still reports
            # success.
            self.previous_species = self._confirmed_species_matching(
                self.species,
            )
            if self.previous_species is None:
                return self.json_error(
                    f'"{self.species}" is not a confirmed species of the '
                    "submitted photos",
                )
            self.species = self.previous_species
        elif self.add_mode:
            self.previous_species = None
        elif requested_previous is not None:
            requested_previous = normalize_keyword_display(
                str(requested_previous),
            )
            if not requested_previous:
                return self.json_error("previous_species must not be empty")
            # The named species must be one the burst is currently confirmed
            # as — including when nothing is confirmed (or there is no
            # cache), otherwise a stale client could have an arbitrary
            # keyword untagged from the submitted photos.
            self.previous_species = self._confirmed_species_matching(
                requested_previous,
            )
            if self.previous_species is None:
                return self.json_error(
                    f'"{requested_previous}" is not a confirmed species '
                    "of the submitted photos",
                )
        else:
            self.previous_species = (
                self.current_species_list[0]
                if self.current_species_list else None
            )
        return None

    # -- previous species rows ---------------------------------------

    def resolve_old_species_rows(self) -> None:
        """Resolve which keyword row stands for the previous species per photo."""
        previous_species = self.previous_species
        # Compare using the ASCII-only fold SQLite's `COLLATE NOCASE` (which
        # add_keyword's dedupe relies on) uses — Python's str.lower() folds
        # non-ASCII letters that SQLite treats as distinct. Without this, a
        # cache-recorded confirmed species `Éclair` and a user-submitted
        # `éclair` would match here, the replacement path would be skipped,
        # yet add_keyword's NOCASE lookup would still create/tag a separate
        # `éclair` row — leaving the photo with both taxonomy tags and no
        # remove queued for the old one.
        self.is_replacement = self.remove_mode or (
            previous_species is not None
            and keyword_match_key(previous_species)
            != keyword_match_key(self.species)
        )
        if not self.is_replacement:
            return
        # Match add_keyword's write path: species keywords live as root
        # keywords (parent_id IS NULL). Looking up by name alone could
        # collide with a non-species homonym nested under another keyword
        # (schema allows UNIQUE(name, parent_id)). Accept taxonomy-typed
        # rows with is_species=0 too: update_keyword with an explicit
        # type='taxonomy' (the Keywords type dropdown) doesn't set the
        # legacy is_species column, and the rest of the app treats
        # (is_species = 1 OR type = 'taxonomy') as species.
        self.old_kid_row = self.db.keyword_taxa.species_root_by_name(self._replaced_species)
        if self.old_kid_row is not None and self.old_kid_row["taxon_id"] is None:
            # Unlinked root: ``previous_species`` names a specific
            # legacy row whose identity is that row's own id, not a
            # taxon. Assign it to every submitted photo — the removal
            # loop below relies on this exact-row identity to route
            # through the ``eff_taxon_id is None`` branch of its
            # homonym-conflict guard, which distinguishes the unlinked
            # legacy species from any linked same-name row on the
            # photo (a distinct species that must be preserved). A
            # per-photo resolution would pick up the attached linked
            # homonym row and treat that linked species AS the
            # previous species, queueing it for removal.
            for pid in self.photo_ids:
                self.per_photo_old_row[pid] = self.old_kid_row
        else:
            self._resolve_per_photo_old_rows()

    def _resolve_per_photo_old_rows(self) -> None:
        """Resolve the previous species from each photo's attached rows.

        Linked root (its identity IS the taxon, and taxon-based
        matching in the removal loop is what actually decides
        equivalence) OR no root at all: resolve per-photo from
        the rows actually attached to each submitted photo. A
        single catalog-wide root assignment would map every
        submitted photo to that root's taxon; the removal loop
        matches attached rows by ``taxon_id``, so any submitted
        photo whose only same-name tag is a hierarchy leaf
        under a different taxon (a legitimate homonym —
        duplicate repair deliberately preserves these rows)
        would keep its old leaf attached while the new species
        is added on top, leaving a stale duplicate. The
        per-photo query naturally covers both attached shapes:
        its ``ORDER BY parent_id IS NULL`` puts an attached
        root first (fast common case) and otherwise picks the
        attached hierarchy leaf on the photo's own taxon.

        Filter to species-rank (or NULL-rank) taxonomy-typed
        rows and accept ``is_species = 1`` rows too — for the
        same taxonomy-typed vs. legacy-species reason as the
        root lookup.

        When a linked root exists, ALSO match rows whose
        ``taxon_id`` equals the root's taxon even if the leaf
        display name differs from ``previous_species`` — this
        is how repaired hierarchy leaves (e.g. ``Desert Verdin``
        under root ``Verdin`` after duplicate repair detached
        the redundant root) get resolved. Without it, the
        removal loop can't find any old row on the photo and
        the endpoint records a plain add, leaving the photo
        tagged with both the old leaf and the new species.
        """
        linked_root_taxon = (
            self.old_kid_row["taxon_id"] if self.old_kid_row is not None else None
        )
        self.old_kid_row = None
        # Match by the linked root's taxon (when there is one) OR by
        # ``previous_species`` name. The taxon arm catches repaired aliases
        # (e.g. ``Desert Verdin`` sharing the root's taxon); the name arm
        # preserves the existing behavior for attached same-name rows under
        # a different taxon (a legitimate homonym duplicate repair leaves
        # alone).
        candidate_rows = self.db.photo_species.previous_species_candidates(
            self.photo_ids, self._replaced_species, linked_root_taxon,
        )
        for row in candidate_rows:
            # SQL orders root rows first, then by id — first
            # candidate per photo is the most canonical.
            self.per_photo_old_row.setdefault(row["photo_id"], row)
        if self.per_photo_old_row:
            # ``old_kid_row`` gates the removal block below.
            # ``old_target_key`` is derived from
            # ``previous_species`` (not this representative row)
            # so the NULL-taxon fallback in the removal loop
            # stays keyed to the requested display name even
            # when the resolved row is a differently-named
            # hierarchy alias.
            self.old_kid_row = next(iter(self.per_photo_old_row.values()))

    # -- keyword writes (inside the transaction) ---------------------

    def resolve_target_keyword(self) -> None:
        """Resolve the target species keyword and which photos already carry it.

        Resolve the target species keyword id up front so we can precheck
        which photos already carry it. add_keyword is idempotent and
        returns the existing id when the species already exists.
        """
        db = self.db
        if self.remove_mode:
            # Nothing is being added: never create a keyword row just to
            # remove it. Report the existing root's id when one exists.
            stored = db.keyword_taxa.species_root_by_name(
                self.species, prefer_taxonomy=True,
            )
            self.kid = stored["id"] if stored else None
            self.already_has_new = set()
            self.newly_tagged = []
            return
        self.kid = (self.preserved_keyword_id if self.preserved_keyword_id is not None
                    else db.add_keyword(self.species, is_species=True, _commit=False))
        # Queue/record the stored spelling (see api_add_keyword).
        stored = db.keywords.name_of(self.kid)
        if stored:
            self.species = stored

        # A species can already be attached through a hierarchical
        # keyword row with a different id/casing. Compare by taxon_id
        # (or normalized name for taxonomy-less legacy rows), otherwise
        # a confirmation creates a redundant top-level association.
        self.already_has_new = db.get_photos_with_equivalent_species(
            self.photo_ids, self.kid,
        )
        self.newly_tagged = [
            pid for pid in self.photo_ids if pid not in self.already_has_new
        ]

    def untag_previous_species(self, ws_id: int) -> None:
        """Untag the previous species and queue its sidecar removes.

        Resolve every attached keyword row equivalent to the previous
        species, including nested hierarchy leaves. Multiple hierarchy
        placements are deliberate and survive duplicate repair, so a
        replacement must remove and record all of them for undo/redo.
        """
        if self.is_replacement and self.old_kid_row is not None:
            # ``old_target_key`` keys the NULL-taxon fallback in the
            # removal loop below. Derive it from ``previous_species``
            # (the requested display name) rather than the resolved
            # representative row's stored name — with the taxon-based
            # per-photo resolution above, that row can be a hierarchy
            # alias whose leaf name differs from what the user typed
            # (e.g. resolved ``Desert Verdin`` for requested
            # ``Verdin``), and the NULL-taxon fallback should still
            # match a legacy ``Verdin`` row on the photo.
            old_target_key = keyword_match_key(self._replaced_species)
            homonym_conflicts = _HomonymConflicts(self.db, old_target_key)
            self._collect_old_rows(old_target_key, homonym_conflicts)
            self._restore_lost_equivalence()
            self._untag_old_rows(ws_id)

        self.had_old = set(self.old_rows_by_photo)

    def _collect_old_rows(self, old_target_key: str, homonym_conflicts: _HomonymConflicts) -> None:
        """Group every attached row equivalent to the previous species by photo."""
        for row in self.db.photo_species.attached_species_rows(self.photo_ids):
            photo_old = self.per_photo_old_row.get(row["photo_id"])
            if photo_old is None:
                # ``previous_species`` did not resolve to any
                # row attached to this photo (no root-lookup
                # hit, no candidate leaf); nothing to remove.
                continue
            if _is_previous_species_row(
                row, photo_old, old_target_key, homonym_conflicts,
            ):
                self.old_rows_by_photo.setdefault(row["photo_id"], []).append(row)

    def _restore_lost_equivalence(self) -> None:
        """Re-tag photos whose only equivalent rows are about to be untagged.

        Same-taxon replacements (e.g., renaming to a scientific-name
        alias) make the earlier taxon-equivalence precheck stale:
        every equivalent row is scheduled for removal, so the photo
        would end up carrying no species keyword. Recompute
        equivalence while ignoring the rows about to be untagged and
        move any now-uncovered photo back into ``newly_tagged`` so
        the tag_photo/keyword_add loop below still writes ``kid``.
        """
        excluded_ids = {
            row["id"]
            for rows in self.old_rows_by_photo.values()
            for row in rows
        }
        if not excluded_ids:
            return
        replacement_covered = list(
            self.already_has_new & set(self.old_rows_by_photo)
        )
        if not replacement_covered:
            return
        survivors = self.db.get_photos_with_equivalent_species(
            replacement_covered, cast(int, self.kid),
            exclude_keyword_ids=excluded_ids,
        )
        lost_equivalence = set(replacement_covered) - survivors
        if lost_equivalence:
            self.already_has_new -= lost_equivalence
            self.newly_tagged = [
                pid for pid in self.photo_ids
                if pid not in self.already_has_new
            ]

    def _untag_old_rows(self, ws_id: int) -> None:
        for pid, old_rows in self.old_rows_by_photo.items():
            remove_names = []
            for old in old_rows:
                self.db.untag_photo(pid, old["id"], _commit=False, _record_history=True)
                # A same-taxon alias replace re-adds ``kid`` right
                # after, so its sidecar remove is skipped; a remove
                # must queue every untagged row.
                if (self.remove_mode or old["id"] != self.kid) and (
                    old["name"] not in remove_names
                ):
                    remove_names.append(old["name"])
            for old_name in remove_names:
                queue_keyword_remove(
                    self.db, pid, old_name, workspace_id=ws_id, _commit=False,
                )

    def tag_new_species(self, ws_id: int) -> None:
        for pid in self.newly_tagged:
            self.db.tag_photo(pid, cast(int, self.kid), source="manual", _commit=False,
                              provenance=self.provenance, _record_provenance=True)
            queue_keyword_add(
                self.db, pid, self.species, workspace_id=ws_id, _commit=False,
            )

        if self.provenance is not None and not self.remove_mode and not self.preserve_existing_provenance:
            from services.species_provenance import remember_before
            # Equivalent hierarchy aliases must retain their actual keyword ID.
            # Not remove mode, so resolve_target_keyword stored the new keyword id.
            equivalent = self.db.photo_species.equivalent_assignment_ids(self.already_has_new, cast(int, self.kid))
            for pid, keyword_ids in equivalent.items():
                for keyword_id in keyword_ids:
                    remember_before(self.db, pid, keyword_id, force=True)
                    self.db.species_provenance.set(pid, keyword_id, self.provenance)
                self.attribution_only.append(pid)

    def record_photo_edit(self) -> None:
        """Record the keyword change in edit history for undo."""
        kid = self.kid
        if self.is_replacement and self.had_old:
            # Photos that actually changed: had the old keyword (so the
            # remove side fired) and/or newly gained the new one. Use the
            # union so undo restores the exact state we mutated.
            newly_set = set(self.newly_tagged)
            changed = [
                pid for pid in self.photo_ids
                if pid in self.had_old or pid in newly_set
            ]
            changed = list(dict.fromkeys(changed + self.attribution_only))
            items = []
            for pid in changed:
                old_ids = [
                    row["id"] for row in self.old_rows_by_photo.get(pid, [])
                ]
                if len(old_ids) > 1:
                    old_value = json.dumps({
                        "keyword_id": old_ids[0],
                        "keyword_ids": old_ids,
                    }, sort_keys=True)
                else:
                    old_value = str(old_ids[0]) if old_ids else ""
                items.append({
                    "photo_id": pid,
                    "old_value": old_value,
                    "new_value": str(kid) if pid in newly_set else "",
                })
            if self.remove_mode:
                description = (
                    f'Removed species "{self.species}" from {len(changed)} photos'
                )
            else:
                description = (
                    f'Replaced species "{self.previous_species}" with '
                    f'"{self.species}" on {len(changed)} photos'
                )
            self.photo_edit_id = self.db.record_edit(
                "species_replace",
                description,
                str(kid) if kid is not None else "",
                items,
                is_batch=len(changed) > 1,
                _commit=False,
                source=(self.provenance or {}).get('reviewer'),
                reason=(self.provenance or {}).get('reason'),
            )
        elif self.newly_tagged or self.attribution_only:
            items = [
                {"photo_id": pid, "old_value": "", "new_value": str(kid)}
                for pid in self.newly_tagged
            ]
            items.extend({"photo_id": pid, "old_value": '{"no_tag": true}', "new_value": str(kid)}
                         for pid in self.attribution_only)
            self.photo_edit_id = self.db.record_edit(
                "keyword_add",
                f'Confirmed species "{self.species}" on '
                f'{len(items)} photos',
                str(kid),
                items,
                is_batch=len(items) > 1,
                _commit=False,
                source=(self.provenance or {}).get('reviewer'),
                reason=(self.provenance or {}).get('reason'),
            )

    # -- pipeline cache (inside the transaction) ---------------------

    def compute_cache_state(self) -> None:
        """Work out the confirmed list and the cache fields this edit writes."""
        from pipeline_results import updated_species_list

        # The confirmed set after this edit, for the cache, the history
        # entry and the response.
        self.new_species_list = updated_species_list(
            self.current_species_list, self.species, self.previous_species,
            add=self.add_mode, remove=self.remove_mode,
        )
        if self.remove_mode:
            self.cache_description = f'Removed species "{self.species}" from '
        elif self.add_mode:
            self.cache_description = f'Added species "{self.species}" on '
        else:
            self.cache_description = f'Confirmed species "{self.species}" on '
        if not (self.cached and self.target_enc is not None):
            return
        # The override's confirmed state comes from the database (read
        # inside this transaction, so it sees the tags just written),
        # the same rule serialize_results applies on regroup: a burst
        # is confirmed as the species every frame carries, and only
        # when every entry of the edited list is on every frame. Per-
        # frame extras ([A, C] beside [A]) do not unconfirm it — the
        # user just confirmed the set on all of it — and the list is
        # the cache's ordering plus any extra the frames all share
        # that the cache had not recorded. An edit that leaves some
        # frame without the full set records the list unconfirmed;
        # it still stays authoritative (burst_species_list), because
        # None would make the burst inherit the encounter's stale
        # list and resurrect a species this edit removed. An emptied
        # burst keeps an explicit empty override for the same reason.
        frames_share, shared_keys, recorded = self._read_frame_species()
        if self.burst_index is not None:
            self.new_burst_override = self._new_burst_override(
                frames_share, shared_keys, recorded,
            )
            self.will_auto_detach = self._burst_will_auto_detach()
        else:
            self.new_encounter_state = {
                "confirmed_species": (
                    self.new_species_list[0] if self.new_species_list else None
                ),
                "confirmed_species_list": list(self.new_species_list),
                "species_confirmed": (
                    bool(self.new_species_list) and frames_share
                ),
            }

    def _read_frame_species(self) -> tuple[bool, set[str], set[str]]:
        """Whether every frame carries the edited list, and the shared keys."""
        from pipeline_results import species_key_set

        self.actual_by_photo = self.db.photo_species.get_species_for_photos(
            self.photo_ids,
        )
        actual_sets = [
            species_key_set(self.actual_by_photo.get(pid, []))
            for pid in self.photo_ids
        ]
        shared_keys = (
            set.intersection(*actual_sets) if actual_sets else set()
        )
        recorded = species_key_set(self.new_species_list)
        frames_share = (
            bool(actual_sets)
            and all(actual_sets)
            and bool(shared_keys)
            and recorded <= shared_keys
        )
        return frames_share, shared_keys, recorded

    def _new_burst_override(self, frames_share: bool, shared_keys: set[str], recorded: set[str]) -> JsonDict | None:
        from pipeline_results import (
            build_species_override,
            empty_species_override,
        )

        new_species_list = self.new_species_list
        if not new_species_list:
            return empty_species_override()
        if frames_share:
            extras = [
                s for s in self.actual_by_photo.get(self.photo_ids[0], [])
                if keyword_match_key(s) in shared_keys
                and keyword_match_key(s) not in recorded
            ]
            return build_species_override(
                new_species_list + extras,
            )
        return build_species_override(
            new_species_list, confirmed=False,
        )

    def _burst_will_auto_detach(self) -> bool:
        """Whether the edited burst splits out of its encounter.

        Auto-detach if the burst's species no longer overlap
        its encounter's — splits it out and merges into an
        adjacent encounter of the same confirmed species when
        one exists. Compare with keyword_match_key so a cached
        pre-normalization spelling (e.g. ‘Apapane) does not
        trigger a needless split against the stored species
        (Apapane); the DB write already normalized to the
        canonical form. Adding a second species never
        detaches (it can only widen overlap); a remove
        detaches only when it leaves a non-empty set that
        shares nothing with the encounter. An emptied burst
        stays put: there is nothing to file it under.
        """
        from pipeline_results import (
            encounter_confirmed_species_list,
            species_key_set,
        )

        # compute_cache_state only asks once the cached encounter is found.
        target_enc = cast("JsonDict", self.target_enc)
        enc_species_list = encounter_confirmed_species_list(target_enc)
        if not enc_species_list and target_enc.get("species"):
            enc_species_list = [target_enc["species"][0]]
        return (
            not self.add_mode
            and bool(self.new_species_list)
            and bool(enc_species_list)
            and not (
                species_key_set(self.new_species_list)
                & species_key_set(enc_species_list)
            )
            and len(target_enc["bursts"]) > 1
        )

    def record_cache_only_confirmation(self) -> None:
        """Give a confirmation that only changes the cache its history entry."""
        if self.preview_scope:
            return
        cache_only_write = (
            not (self.is_replacement and self.had_old)
            and not self.newly_tagged
            and self.photo_edit_id is None
            and self.cached
            and self.target_enc is not None
        )
        if not cache_only_write:
            return
        # No keyword row was recorded (every photo already carries
        # this species), but the cache mutation below will still
        # write ``confirmed_species`` / ``species_confirmed`` (or a
        # burst ``species_override``). Without a matching history
        # entry, a preceding grouping edit would remain the newest
        # undoable row and its undo would silently discard this
        # confirmation because grouping signatures ignore these
        # species fields by design. Record the cache-only write so
        # LIFO undo reverts it first. When auto-detach will fire
        # the structural change and confirmation share one grouping
        # history entry further down instead.
        from pipeline_results import encounter_confirmed_species_list
        from services.grouping_history import (
            record_species_confirm_cache,
        )

        # ``cache_only_write`` required the cached encounter.
        target_enc = cast("JsonDict", self.target_enc)
        if self.burst_index is not None:
            burst_target = target_enc["bursts"][self.burst_index]
            current_override = burst_target.get("species_override")
            if (
                current_override != self.new_burst_override
                and not self.will_auto_detach
            ):
                self.undo_edit_id = record_species_confirm_cache(
                    self.db, species=self.species, target_enc=target_enc,
                    burst_index=self.burst_index,
                    submitted_photo_ids=self.photo_ids,
                    new_override=self.new_burst_override,
                    description=self.cache_description + "1 burst",
                )
        else:
            current_state = {
                "confirmed_species": target_enc.get("confirmed_species"),
                "confirmed_species_list": list(
                    encounter_confirmed_species_list(target_enc)
                ),
                "species_confirmed": bool(target_enc.get("species_confirmed")),
            }
            if current_state != self.new_encounter_state:
                self.undo_edit_id = record_species_confirm_cache(
                    self.db, species=self.species, target_enc=target_enc,
                    burst_index=None,
                    submitted_photo_ids=self.photo_ids,
                    new_encounter_state=self.new_encounter_state,
                    description=(
                        self.cache_description + f"{len(self.photo_ids)} photos"
                    ),
                )

    def apply_cache_mutation(self) -> None:
        """Stage cache and history changes inside the caller's transaction.

        The shared commit helper persists the cache and restores it on failure.
        Legacy requests use the validated burst/encounter scope. Previewed
        requests derive summaries after removals establish final membership.
        """
        cached = self.cached
        if not (cached and self.target_enc is not None):
            return
        flag_items = []
        group_attribution: dict = {}
        if self.group_plan is not None:
            from services.review_group_changes import apply_group_changes
            # Derive shared species only after removals establish final bursts.
            group_result = apply_group_changes(self.db, cached, self.group_plan)
            flag_items = group_result["items"]
            group_attribution = group_result
        if self.preview_scope:
            from services.encounter_confirmation import apply_scope
            apply_scope(self.db, cached, self.target_enc, self.photo_ids,
                        split=self.split, preferred_order=self.new_species_list)
        elif self.burst_index is not None:
            self._apply_burst_override()
        else:
            self._apply_encounter_species()
        # A deep copy of ``cached``, taken when it was loaded.
        before_cached = cast("JsonDict", self.before_cached)
        if self.group_plan is not None:
            self._apply_combined_group(flag_items, group_attribution)
        elif (
            self.photo_edit_id is not None
            and before_cached["encounters"] != cached["encounters"]
        ):
            # Labels and confirmation counts are part of the same user
            # action even when no burst moves to another encounter.
            # The edit row this request recorded, read in its transaction.
            photo_edit = cast("Row", self.db.edit_history.action_and_new_value(self.photo_edit_id))
            change = {
                "before": before_cached["encounters"],
                "after": cached["encounters"],
                "photo_edit": dict(photo_edit),
            }
            from services.grouping_history import convert_to_grouping_edit
            convert_to_grouping_edit(self.db, self.photo_edit_id, change)
        if (self.group_plan is None and self.preview_scope and self.photo_edit_id is None
                and before_cached["encounters"] != cached["encounters"]):
            from services.grouping_history import record_grouping_edit
            self.undo_edit_id = record_grouping_edit(self.db, self.cache_description + f"{len(self.photo_ids)} photos", {
                "before": before_cached["encounters"], "after": cached["encounters"],
            })

    def _loaded_cache(self) -> tuple[JsonDict, JsonDict, JsonDict]:
        """``(cached, before_cached, target_enc)`` for the cache-mutation helpers.

        ``apply_cache_mutation`` calls them only once the cache is loaded and
        holds the target encounter; ``before_cached`` is the cache's deep copy.
        """
        return (
            cast("JsonDict", self.cached), cast("JsonDict", self.before_cached),
            cast("JsonDict", self.target_enc),
        )

    def _apply_combined_group(self, flag_items: list[JsonDict], group_attribution: dict) -> None:
        """Record the completed species/cull decision as one history entry."""
        from services.grouping_history import convert_to_grouping_edit, record_grouping_edit

        cached, before_cached, _ = self._loaded_cache()
        # Called only with a group plan, whose photos validate_group_changes checked.
        group_plan = cast("JsonDict", self.group_plan)
        self.group_photo_states = {
            pid: {"flag": cast("Row", self.db.photos.get(pid))["flag"] or "none"}
            for key in ("picks", "rejects", "candidates") for pid in group_plan[key]
        }
        change = {"before": before_cached["encounters"], "after": cached["encounters"],
                  "label_edit": True, "flag_items": flag_items}
        if self.photo_edit_id is not None:
            # The edit row this request recorded, read in its transaction.
            photo_edit = cast("Row", self.db.edit_history.action_and_new_value(self.photo_edit_id))
            change["photo_edit"] = dict(photo_edit)
            convert_to_grouping_edit(self.db, self.photo_edit_id, change)
            self._record_group_flag_attribution(self.photo_edit_id, group_attribution)
        elif flag_items or change["before"] != change["after"]:
            if flag_items:
                change["photo_edit"] = {"action_type": "flag", "new_value": "pipeline_group_apply"}
            self.undo_edit_id = record_grouping_edit(self.db, "Applied Group Review species and cull decisions", change, flag_items)
            self._record_group_flag_attribution(self.undo_edit_id, group_attribution)

    def _record_group_flag_attribution(self, edit_id: int | None, group_attribution: JsonDict) -> None:
        """Attach the pre-write attribution snapshot to the grouping edit.

        apply_grouping_photo_edit's undo iterates ``flag_items`` and calls
        ``restore_flag_attribution_history`` for each, so the Group Review
        combined apply reverses attribution the same way pipeline group
        apply does.
        """
        touched_ids = group_attribution.get("touched_ids") or []
        if not edit_id or not touched_ids:
            return
        self.db.photo_review.record_flag_attribution_history(
            edit_id, touched_ids,
            group_attribution.get("before_attributions") or {},
            self.db.photo_review.flag_attributions(touched_ids),
        )

    def _apply_burst_override(self) -> None:
        cached, before_cached, target_enc = self._loaded_cache()
        # A burst-scoped request: burst_error validated the index.
        burst_index = cast(int, self.burst_index)
        target_enc["bursts"][burst_index]["species_override"] = (
            self.new_burst_override
        )
        if not self.will_auto_detach:
            return
        auto_detach_burst_for_species(
            cached, self.target_enc_idx, burst_index,
            self.new_species_list[0],
        )
        change = {
            "before": before_cached["encounters"],
            "after": cached["encounters"],
        }
        if self.photo_edit_id is None:
            from services.grouping_history import record_grouping_edit
            self.undo_edit_id = record_grouping_edit(
                self.db, self.cache_description + "1 burst", change, [],
            )

    def _apply_encounter_species(self) -> None:
        from pipeline_results import set_encounter_confirmed_species

        _, _, target_enc = self._loaded_cache()
        # compute_cache_state set it for an encounter-scoped request.
        new_encounter_state = cast("JsonDict", self.new_encounter_state)
        set_encounter_confirmed_species(target_enc, self.new_species_list)
        target_enc["species_confirmed"] = bool(
            new_encounter_state["species_confirmed"]
        )
        # Child bursts may carry overrides serialize_results (or
        # an earlier burst edit) materialized for the pre-edit
        # species. Both review pages read those before the
        # encounter, so leaving them would show the old species
        # on every burst and let a flag-only Rapid apply re-tag
        # it. Rebuild each fully-submitted burst's override from
        # what its frames carry now (read inside this
        # transaction); bursts without an override keep
        # inheriting the encounter's new list.
        from pipeline import derive_burst_override
        submitted = set(self.photo_ids)
        for child in target_enc.get("bursts") or []:
            if child.get("species_override") is None:
                continue
            child_ids = child.get("photo_ids") or []
            if not child_ids or not set(child_ids) <= submitted:
                continue
            child["species_override"] = derive_burst_override(
                [
                    {"confirmed_species_list": self.actual_by_photo.get(pid, [])}
                    for pid in child_ids
                ],
                preferred_order=self.new_species_list,
            )

    # -- response ----------------------------------------------------

    def response(self, low_confidence_photo_ids: list[int], response_mode: str = "full") -> JsonDict:
        """The JSON body for a landed confirmation."""
        # Report `replaced` consistent with the actual replacement decision
        # (is_replacement, which uses keyword_match_key to match SQLite's
        # ASCII-only NOCASE). Python's `.lower()` folds non-ASCII pairs like
        # `Éclair`/`éclair` — which SQLite/add_keyword keep as distinct
        # keyword rows — so a `.lower()` comparison here would report
        # replaced=None on a request that actually untagged the previous
        # species row and tagged a new one.
        replaced = (
            self.previous_species
            if self.is_replacement and not self.remove_mode else None
        )
        response = {
            "ok": True,
            "species": self.species,
            "keyword_id": self.kid,
            "photo_count": len(self.photo_ids),
            "previous_species": replaced,
            "mode": (
                "remove" if self.remove_mode
                else "add" if self.add_mode else "replace"
            ),
            "species_list": self.new_species_list,
            "low_confidence_photo_ids": low_confidence_photo_ids,
            # Kept for older clients/tests that checked this field; explicit
            # confirmations no longer skip submitted photos.
            "skipped_photo_ids": [],
        }
        if self.group_photo_states is not None:
            response["group_photos"] = self.group_photo_states
        response["response_mode"] = response_mode
        if response_mode == "compact":
            from services.species_assignment_results import assignment_result, encounter_changes, undo_reference
            response.update(assignment_result(
                self.db, self.kid, self.photo_ids,
                added=self.newly_tagged, removed=self.had_old, low_confidence=low_confidence_photo_ids,
            ))
            response["encounter_changes"] = encounter_changes(self.before_cached, self.cached)
            response["cache_changed"] = bool(response["encounter_changes"]["removed"] or response["encounter_changes"]["upserts"])
            response["undo"] = undo_reference(self.undo_edit_id or self.photo_edit_id)
            response["summary"] = (self.cached or {}).get("summary", {})
        elif self.cached:
            response["encounters"] = self.cached.get("encounters", [])
            response["summary"] = self.cached.get("summary", {})
        return response
