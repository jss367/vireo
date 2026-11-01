"""Highlights and photo preferences.

The ``/api/highlights*`` routes back the Highlights page: the bucketed
per-species payload, one bucket's photo pages, confirming a photo's top
prediction, relabelling photos (with the curation migration that follows
the species), and saving a selection as a collection. ``/api/species-highlights*``
adds, removes and reorders a species' ordered highlight photos, and
``/api/photo-preferences`` sets or clears a species' representative (or,
for the legacy ``purpose=highlights``, an ordered highlight).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from flask import Blueprint, jsonify, request
from highlights_payload import (
    HighlightsQuery,
    TextMatch,
    apply_highlight_preferences,
    apply_ordered_highlights,
    bucket_best_score,
    build_highlights_payload,
    collect_highlight_buckets,
    filter_highlight_curation_state,
    filter_highlight_sections,
    normalize_highlight_confirmation_filter,
    species_canonicalizer,
)
from keyword_normalization import keyword_match_key, normalize_keyword_display
from photo_payload import attach_edit_recipes
from services import prediction_decisions
from services.pending_changes import queue_keyword_add, queue_keyword_remove
from species_memberships import photo_species_memberships
from web.request_args import request_bool_arg

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask import Response
    from flask.typing import ResponseReturnValue
    from services.service_types import JsonDict, JsonError
    from web.web_types import GetDb


class _HighlightsRelabel:
    """One ``/api/highlights/relabel`` decision, run under the
    prediction-decision lock by ``_highlights_relabel_under_lock``.

    ``snapshot_curation()`` reads the curation the relabel moves before any
    write; ``retag_photos()``, ``rename_curation()`` and ``record_relabel_edit()``
    then write inside the caller's transaction.
    """

    def __init__(
        self, db: Database, photo_ids: list[int], species: str, top_predictions: dict[int, Row],
        from_species: str | None = None,
    ) -> None:
        self.db = db
        self.photo_ids = photo_ids
        self.species = species
        self.top_predictions = top_predictions
        # A relabel from one species bucket replaces only that species on a
        # photo that carries several; None relabels the photo's whole species.
        self.from_species = from_species
        self._canonicalize = species_canonicalizer(db)
        self._from_key = (
            _species_key(self._canonicalize, from_species)
            if from_species else None
        )
        self.ws_id = None
        self.predicted_species_by_pid = {}
        self.current_species_by_pid = {}
        self.highlight_renames = {}
        self.hl_prev_by_pid = {}
        # Photos that already had a `(species=<target>, photo_id)` row in
        # species_highlights before the relabel. rename_species_highlights_species
        # skips inserting a duplicate for these, so the destination row is
        # pre-existing and undo must not delete it.
        self.hl_dst_preexisting = set()
        self.pref_dst_taken = set()
        self.rep_dst_preexisting = set()
        self.rep_selected_order_by_pid_species = {}
        self.preference_renames = {}
        self.pref_prev_by_pid = {}
        self.pref_covered_by_pid_species = set()
        self.representative_renames = {}
        self.rep_prev_by_pid = {}
        self.items = []
        self.rejected_prediction_ids = []
        self.has_old_species = False

    def snapshot_curation(self) -> None:
        self._snapshot_predicted_species()
        self.ws_id = self.db.require_workspace_id()
        # The per-photo snapshot reads chunk their IN lists: photo_ids has
        # no upstream cap (_parse_highlight_photo_ids just parses the list),
        # so a bulk relabel of >999 photos would otherwise blow
        # SQLITE_MAX_VARIABLE_NUMBER on legacy SQLite builds.
        self._snapshot_current_species()
        self._snapshot_highlights()
        self._snapshot_destination_slots()
        self._snapshot_preferences()
        self._snapshot_representatives()

    def _snapshot_predicted_species(self) -> None:
        """Snapshot the top-prediction species per photo, keyed by
        keyword_match_key (SQLite's ASCII-only NOCASE fold). Used in
        ``_accept_curation_source``'s current_species-empty filter branch
        to accept only curation whose old species matches an active
        prediction — see the prediction-only relabel scenario in
        test_highlights_relabel_prediction_only_undo_restores_curation.
        keyword_match_key (not Python's str.lower()) matches
        add_keyword's SQLite dedupe, so `Éclair` vs `éclair` — which
        SQLite/add_keyword keep distinct — stay distinct here too;
        otherwise a stale curation row for one would fold onto the
        other on relabel.

        Track both the raw prediction label and its canonicalized form
        via ``resolve_species_display_name``: highlight buckets
        canonicalize hierarchy aliases through that resolver before
        saving (e.g. an unconfirmed prediction ``Desert Verdin`` is
        stored under its unique-taxon root ``Verdin``). Without the
        canonical key, ``_accept_curation_source`` would reject the
        saved ``Verdin`` highlight/representative row as stale when
        only the raw prediction key is in the set, leaving that
        curation stranded under the old bucket after relabel.
        """
        predicted_species_by_pid = self.predicted_species_by_pid
        for pid_pred, pred_row in self.top_predictions.items():
            pred_species = pred_row["species"] if pred_row else None
            if not pred_species:
                continue
            raw_key = keyword_match_key(pred_species)
            if raw_key:
                predicted_species_by_pid.setdefault(pid_pred, set()).add(raw_key)
            canonical = self.db.resolve_species_display_name(pred_species)
            if canonical and canonical != pred_species:
                canonical_key = keyword_match_key(canonical)
                if canonical_key:
                    predicted_species_by_pid.setdefault(pid_pred, set()).add(canonical_key)

    def _snapshot_current_species(self) -> None:
        """Snapshot each photo's current species-rank taxonomy keywords
        before the relabel touches them. Used to skip stale curation rows
        for species the photo no longer carries across all three curation
        tables (species_highlights, photo_preferences,
        species_representatives): untag_photo does not clear any of them,
        so a photo can retain a curation row for species A after that
        keyword was removed. Without this filter, relabeling the photo's
        current species B→C would sweep the stale A rows into A→C
        renames — losing the preserved A state and making C look
        manually selected.

        Species-rank filter matches the old_rows removal query in
        ``_retag_photo`` — higher-rank taxonomy keywords (genus/family) are
        not untagged by the relabel, so treating them as "current" would
        migrate their curation onto the new species while leaving the
        higher-rank keyword attached with its curation stripped.

        Route through get_species_keywords_for_photos so taxon-linked
        hierarchy leaves canonicalize to the same-taxon root spelling
        (e.g. an attached ``Desert Verdin`` reads as ``Verdin``).
        Existing curation is keyed under that root, so a raw-name
        snapshot would reject the root-key highlight/representative row
        as stale after duplicate repair left only the hierarchy leaf,
        stranding it under a species the photo no longer carries by
        name.
        """
        current_species_by_pid = self.current_species_by_pid
        for pid, names in self.db.photo_species.get_species_for_photos(
            self.photo_ids
        ).items():
            for name in names:
                if not self._is_source_species(name):
                    continue
                key = keyword_match_key(name)
                if key:
                    current_species_by_pid.setdefault(pid, set()).add(key)

    def _is_source_species(self, name: str) -> bool:
        """Whether the relabel replaces ``name``: always for a whole-photo
        relabel, and only the bucket's own species for a scoped one."""
        if self._from_key is None:
            return True
        return _species_key(self._canonicalize, name) == self._from_key

    def _accept_curation_source(self, pid: int, old_species_name: str) -> bool:
        """Shared filter for the highlight, preference, and rep passes.

        With any current taxonomy keywords, only migrate curation for
        species the photo currently carries. Without any taxonomy —
        the prediction-only relabel path (``keyword_add`` instead of
        ``species_replace``) — only migrate curation whose species
        matches an active prediction on the photo. That preserves the
        legitimate prediction-species migration exercised by
        ``test_highlights_relabel_prediction_only_undo_restores_curation``
        while blocking stale curation rows for species that were
        tagged and later untagged.

        Keyed by keyword_match_key (see ``_snapshot_predicted_species``):
        SQLite's ASCII-only NOCASE keeps `Éclair` and `éclair` distinct as
        separate keyword rows, and str.lower() folds them together —
        which would let a stale `éclair` curation row migrate onto a
        photo still carrying `Éclair`.
        """
        old_key = keyword_match_key(old_species_name)
        if not old_key:
            return False
        current = self.current_species_by_pid.get(pid)
        if current:
            return old_key in current
        predicted = self.predicted_species_by_pid.get(pid) or set()
        return old_key in predicted

    def _snapshot_highlights(self) -> None:
        species = self.species
        for row in self.db.species_curation.highlight_rows_for_photos(self.photo_ids):
            old_species_name = row["species"]
            if old_species_name == species:
                self.hl_dst_preexisting.add(row["photo_id"])
                continue
            if not self._accept_curation_source(
                row["photo_id"], old_species_name
            ):
                continue
            self.highlight_renames.setdefault(old_species_name, []).append(
                row["photo_id"]
            )
            # Snapshot the original rank so undo can restore each
            # highlighted photo at its original position instead of
            # dumping it at MAX(rank)+1 (see EditHistoryRepository.restore_relabel_curation).
            self.hl_prev_by_pid.setdefault(row["photo_id"], []).append({
                "species": old_species_name,
                "rank": row["rank"],
            })
        # Backfill dst_existed onto each entry now that the target-species
        # pass has finished (row order within the query is unspecified).
        for pid, entries in self.hl_prev_by_pid.items():
            dst = pid in self.hl_dst_preexisting
            for entry in entries:
                entry["dst_existed"] = dst

    def _snapshot_destination_slots(self) -> None:
        db = self.db
        species = self.species
        # Purposes that already have a row at (new_species, purpose) — for
        # any photo. rename_photo_preferences_species uses INSERT OR IGNORE,
        # so when the destination slot is already taken (either by this
        # photo or a different one), the relabel does not create a new
        # destination row for this photo and undo must not attempt to
        # delete it. Un-gating the old-species restore from a destination
        # row lookup lets undo recover representatives even when the
        # relabel collided with another photo holding the slot.
        self.pref_dst_taken = db.species_curation.preference_purposes_for_species(species)
        # Photos that already had a (species=<target>, photo_id) row in
        # species_representatives before the relabel.
        # species_curation.rename_representatives uses INSERT OR IGNORE, so
        # when the destination rep row is already present the relabel does
        # not create a new one and undo must not delete it. Applies to
        # both preference-covered moves (via pref_prev.rep_dst_existed in
        # _snapshot_preferences) and rep-only moves (via
        # rep_prev.dst_existed).
        self.rep_dst_preexisting.update(
            db.species_curation.representative_photo_ids_for_species(species, self.photo_ids)
        )
        # Snapshot the original selected_order for every existing
        # species_representatives row on the retagged photos. Undo restores
        # each row at its captured order rather than the fresh MAX+1 that
        # species_curation.set_global_representative would assign — so undoing a
        # relabel of a secondary representative does not promote it above
        # the pre-existing primary for that species.
        for row in db.species_curation.representative_rows_for_photos(self.photo_ids):
            self.rep_selected_order_by_pid_species[
                (row["photo_id"], row["species"])
            ] = row["selected_order"]

    def _snapshot_preferences(self) -> None:
        species = self.species
        for row in self.db.species_curation.preference_rows_for_photos(self.photo_ids):
            old_species_name = row["species"]
            if old_species_name == species:
                continue
            if not self._accept_curation_source(
                row["photo_id"], old_species_name
            ):
                continue
            self.preference_renames.setdefault(old_species_name, []).append(
                row["photo_id"]
            )
            self.pref_prev_by_pid.setdefault(row["photo_id"], []).append({
                "purpose": row["purpose"],
                "species": old_species_name,
                "dst_existed": row["purpose"] in self.pref_dst_taken,
                "rep_dst_existed": row["photo_id"] in self.rep_dst_preexisting,
                "rep_selected_order": self.rep_selected_order_by_pid_species.get(
                    (row["photo_id"], old_species_name)
                ),
            })
            self.pref_covered_by_pid_species.add(
                (row["photo_id"], old_species_name)
            )

    def _snapshot_representatives(self) -> None:
        """Global species_representatives moves. Representatives are global
        (no workspace column), so a photo can carry a
        (species, photo_id) row picked from a different workspace with
        no matching photo_preferences row in the active workspace. In
        that case the preference-rename pass (``_snapshot_preferences``)
        would miss it, and the retag would strand the representative under
        the old species name — leaving both species without a
        representative. Query species_representatives directly and enqueue
        any rep-only moves the preference pass didn't already cover. The
        same current-species filter applies here (see
        ``_snapshot_current_species``).
        """
        species = self.species
        for row in self.db.species_curation.representative_rows_for_photos(self.photo_ids):
            old_species_name = row["species"]
            pid = row["photo_id"]
            if old_species_name == species:
                continue
            # rename_photo_preferences_species already migrates
            # species_representatives for pids in preference_renames,
            # so skip anything the preference pass will cover.
            if (pid, old_species_name) in self.pref_covered_by_pid_species:
                continue
            if not self._accept_curation_source(pid, old_species_name):
                continue
            self.representative_renames.setdefault(old_species_name, []).append(pid)
            self.rep_prev_by_pid.setdefault(pid, []).append({
                "species": old_species_name,
                "dst_existed": pid in self.rep_dst_preexisting,
                "selected_order": self.rep_selected_order_by_pid_species.get(
                    (pid, old_species_name)
                ),
            })

    def adopt_stored_spelling(self, kid: int) -> None:
        """Use the stored spelling from here on: add_keyword applies the
        species casing convention, so it can differ from the request
        value, and the queued sidecar changes / curation renames /
        history payload must match the row actually tagged.
        """
        stored = self.db.keywords.name_of(kid)
        if stored:
            self.species = stored

    def retag_photos(self, kid: int) -> None:
        for pid in self.photo_ids:
            self._retag_photo(pid, kid)

    def _retag_photo(self, pid: int, kid: int) -> None:
        db = self.db
        pred = self.top_predictions.get(pid)
        if pred is not None:
            db.update_prediction_status(pred["id"], "rejected", _commit=False)
            self.rejected_prediction_ids.append(pred["id"])

        # A scoped relabel keeps the photo's other species. It still lists
        # the target keyword when the photo already carries it, so undo
        # (which untags the new keyword, then retags these) keeps it.
        old_rows = [
            row for row in db.photo_species.species_rank_keywords_for_photo(pid)
            if row["id"] == kid or self._is_source_species(row["name"])
        ]
        old_primary = old_rows[0] if old_rows else None
        if old_primary is not None:
            self.has_old_species = True
        for old in old_rows:
            db.untag_photo(pid, old["id"], _record_history=True)
            # Compare by keyword id: SQLite's NOCASE is ASCII-only,
            # so `Éclair` and `éclair` live as distinct rows with
            # distinct ids, but Python `.lower()` folds them equal.
            # A name-based skip would suppress the remove for a
            # different SQLite row and leave the old spelling in
            # the sidecar after the next XMP sync (add_keyword
            # normalized `species` to the row it will tag, so `kid`
            # is authoritative).
            if old["id"] != kid:
                queue_keyword_remove(
                    db, pid, old["name"], workspace_id=self.ws_id,
                )

        db.tag_photo(pid, kid, source="manual")
        queue_keyword_add(db, pid, self.species, workspace_id=self.ws_id)
        self.items.append({
            "photo_id": pid,
            "old_value": self._history_old_value(pid, pred, old_primary, old_rows),
            "new_value": str(kid),
        })

    def _history_old_value(self, pid: int, pred: Row | None, old_primary: Row | None, old_rows: list[Row]) -> str:
        old_value = str(old_primary["id"]) if old_primary else ""
        old_keyword_ids = [old["id"] for old in old_rows]
        hl_prev = self.hl_prev_by_pid.get(pid) or []
        pref_prev = self.pref_prev_by_pid.get(pid) or []
        rep_prev = self.rep_prev_by_pid.get(pid) or []
        needs_payload = (
            pred is not None
            or len(old_keyword_ids) > 1
            or hl_prev
            or pref_prev
            or rep_prev
        )
        if needs_payload:
            old_payload = {
                "keyword_id": old_value,
                "keyword_ids": old_keyword_ids,
            }
            if pred is not None:
                old_payload.update({
                    "prediction_id": pred["id"],
                    "prediction_status": pred["status"],
                })
            if hl_prev or pref_prev or rep_prev:
                curation = {
                    "hl_prev": hl_prev,
                    "pref_prev": pref_prev,
                }
                if rep_prev:
                    curation["rep_prev"] = rep_prev
                old_payload["curation"] = curation
            old_value = json.dumps(old_payload, sort_keys=True)
        return old_value

    def rename_curation(self) -> None:
        db = self.db
        species = self.species
        ws_id = self.ws_id
        assert ws_id is not None  # Set by preflight before any relabel writes.
        for old_species_name, pids in self.highlight_renames.items():
            db.rename_species_highlights_species(
                old_species_name,
                species,
                [(pid, ws_id) for pid in pids],
            )
        for old_species_name, pids in self.preference_renames.items():
            db.rename_photo_preferences_species(
                old_species_name,
                species,
                [(pid, ws_id) for pid in pids],
            )
        for old_species_name, pids in self.representative_renames.items():
            db.species_curation.rename_representatives(
                old_species_name,
                species,
                photo_ids=pids,
            )

    def record_relabel_edit(self, kid: int) -> None:
        species = self.species
        action_type = (
            "species_replace"
            if self.has_old_species
            else "keyword_add"
        )
        if action_type == "species_replace" and self.from_species:
            desc = (
                f'Replaced "{self.from_species}" with "{species}" '
                f'on {len(self.photo_ids)} photos'
            )
        elif action_type == "species_replace":
            desc = f'Replaced species with "{species}" on {len(self.photo_ids)} photos'
        else:
            desc = f'Set species "{species}" on {len(self.photo_ids)} photos'
        self.db.record_edit(
            action_type,
            desc,
            str(kid),
            self.items,
            is_batch=len(self.items) > 1,
            _commit=False,
        )


def _species_key(canonicalize: Callable[[str], str | None], name: str) -> str:
    """The match key of ``name``'s canonical spelling, the identity Highlights
    buckets key on."""
    return keyword_match_key(canonicalize(name) or name)


def _parse_optional_species(body: JsonDict, field: str) -> tuple[str | None, str | None]:
    """``(name, error)`` for an optional species string in ``body[field]``."""
    value = body.get(field)
    if value is None:
        return None, None
    if not isinstance(value, str) or not value.strip():
        return None, f"{field} must be a non-empty string"
    return normalize_keyword_display(value.strip()), None


def _subject_prediction(row: Row) -> JsonDict:
    return {
        "detection_id": row["detection_id"],
        "prediction_id": row["id"],
        "species": row["species"],
        "confidence": row["confidence"],
    }


def _top_prediction_confirm_targets(db: Database, photo_ids: list[int]) -> tuple[dict[int, Row], dict[int, str]]:
    """Whole-photo confirm: each photo's top prediction, unless the photo
    already carries any species."""
    top_predictions = db.predictions.top_unrejected_by_photo(photo_ids)
    accepted_photo_ids = db.photo_species.photo_ids_with_species_rank_keyword(photo_ids)
    targets: dict[int, Row] = {}
    skips: dict[int, str] = {}
    for pid in photo_ids:
        if pid in accepted_photo_ids:
            skips[pid] = "already_confirmed"
        elif pid not in top_predictions:
            skips[pid] = "no_prediction"
        else:
            targets[pid] = top_predictions[pid]
    return targets, skips


def _species_confirm_targets(db: Database, photo_ids: list[int], species: str) -> tuple[dict[int, Row], dict[int, str]]:
    """Confirm from one species bucket: the subject prediction that put each
    photo in ``species``'s bucket, so a photo with another species already
    tagged still gets this one, and a photo already tagged with it is
    skipped. Membership follows ``photo_species_memberships``, the rule the
    page used to build the bucket."""
    canonicalize = species_canonicalizer(db)
    target_key = _species_key(canonicalize, species)
    subjects_by_pid = db.predictions.top_unrejected_by_detection(photo_ids)
    names_by_pid = db.photo_species.get_species_for_photos(photo_ids)
    targets, skips = {}, {}
    for pid in photo_ids:
        rows = subjects_by_pid.get(pid, [])
        memberships = photo_species_memberships({
            "species_names": list(names_by_pid.get(pid, [])),
            "subject_predictions": [_subject_prediction(r) for r in rows],
        }, canonicalize)
        membership = next((
            m for m in memberships
            if keyword_match_key(m.species) == target_key
        ), None)
        if membership is not None and membership.accepted_name is not None:
            skips[pid] = "already_confirmed"
        elif membership is None or membership.prediction is None:
            skips[pid] = "no_prediction"
        else:
            pred_id = membership.prediction["prediction_id"]
            targets[pid] = next(r for r in rows if r["id"] == pred_id)
    return targets, skips


def _relabel_source_predictions(db: Database, photo_ids: list[int], from_species: str | None) -> dict[int, Row]:
    """``{photo_id: row}``: the prediction a relabel rejects on each photo.

    A whole-photo relabel rejects the photo's top prediction. A relabel from
    one species bucket rejects only the subject prediction naming that
    species, so the photo's other subjects keep theirs.
    """
    if not from_species:
        return db.predictions.top_unrejected_by_photo(photo_ids)
    canonicalize = species_canonicalizer(db)
    from_key = _species_key(canonicalize, from_species)
    result: dict[int, Row] = {}
    for pid, rows in db.predictions.top_unrejected_by_detection(photo_ids).items():
        match = next((
            r for r in rows
            if _species_key(canonicalize, r["species"]) == from_key
        ), None)
        if match is not None:
            result[pid] = match
    return result


def _confirm_group_key(pred: Row) -> tuple[str, str, str] | tuple[str, int]:
    if pred["group_id"]:
        return ("group", pred["classifier_model"], pred["group_id"])
    return ("prediction", pred["id"])


def _confirm_edit_items(pred: Row, result: JsonDict) -> list[JsonDict]:
    """Edit-history items for one confirm accept.

    Mirror api_accept_prediction: encode ``no_tag`` for entries where the
    photo already carried the target via an equivalent hierarchical/root
    row so undo/redo do not untag a keyword the user deliberately kept.

    Highlights shows ``reviewed`` predictions with a Confirm button, so
    confirming one is a deliberate re-decision. Record that prior status so
    undo puts the row back to ``reviewed`` rather than ``pending``, a status
    it no longer had.
    """
    items: list[JsonDict] = []
    for a in result["affected"]:
        meta: JsonDict = {}
        if not a.get("changed_tag", True):
            meta["no_tag"] = True
        if (a["prediction_id"] == pred["id"]
                and pred["status"] == "reviewed"):
            meta["prior_status"] = "reviewed"
        if meta:
            old_value = json.dumps(
                {"prediction_id": a["prediction_id"], **meta},
            )
        else:
            old_value = str(a["prediction_id"])
        items.append({
            "photo_id": a["photo_id"],
            "old_value": old_value,
            "new_value": str(result["keyword_id"]),
        })
    return items


def _accept_confirm_targets(
    db: Database, photo_ids: list[int], targets: dict[int, Row], skips: dict[int, str], exact_rows: bool,
) -> tuple[list[JsonDict], list[JsonDict]]:
    """Accept each target prediction once per burst group and record the
    edits; ``exact_rows`` limits a grouped accept to the target rows
    themselves, not every row of the group on those photos."""
    pids_by_key = {}
    for pid in photo_ids:
        if pid in targets:
            pids_by_key.setdefault(_confirm_group_key(targets[pid]), []).append(pid)
    processed = set()
    affected = []
    skipped = []
    for pid in photo_ids:
        if pid in skips:
            skipped.append({"photo_id": pid, "reason": skips[pid]})
            continue
        pred = targets[pid]
        key = _confirm_group_key(pred)
        if key in processed:
            continue
        processed.add(key)
        key_pids = pids_by_key[key]
        limits = {"photo_ids": key_pids}
        if exact_rows:
            limits["prediction_ids"] = [targets[p]["id"] for p in key_pids]
        result = db.accept_prediction(pred["id"], _commit=False, **limits)
        if result is None:
            skipped.append({"photo_id": pid, "reason": "prediction_not_found"})
            continue
        if not result["affected"]:
            skipped.extend(
                {"photo_id": confirm_pid, "reason": "prediction_not_found"}
                for confirm_pid in key_pids
            )
            continue
        items = _confirm_edit_items(pred, result)
        desc = f'Accepted prediction: added "{result["species"]}"'
        if len(items) > 1:
            desc += f" to {len(items)} photos"
        db.record_edit(
            "prediction_accept",
            desc,
            str(result["keyword_id"]),
            items,
            is_batch=len(items) > 1,
            _commit=False,
        )
        affected.extend(items)
    return affected, skipped


def create_highlights_blueprint(get_db: GetDb, json_error: JsonError) -> Blueprint:
    """Build the highlights / photo-preferences blueprint.

    The payload builder and highlight-bucket helpers come from
    ``highlights_payload``, shared with the Life List routes and the export
    blueprint's site publishing.

    The confirm and relabel routes are prediction-decision routes: they take
    the shared writer lock through ``services.prediction_decisions``.
    """
    blueprint = Blueprint("highlights", __name__)

    def _parse_highlight_photo_ids(body: JsonDict) -> tuple[list[int], str | None]:
        """``(photo_ids, None)``, or ``([], message)``."""
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return [], "photo_ids required"
        photo_ids: list[int] = []
        seen: set[int] = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return [], "photo_ids must be integers"
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        return photo_ids, None

    def _validate_highlight_photo_ids(db: Database, photo_ids: list[int]) -> tuple[str, int] | None:
        """``(message, status)`` when a photo is unknown or outside the workspace."""
        found_ids = db.photos.existing_ids(photo_ids)
        missing = [pid for pid in photo_ids if pid not in found_ids]
        if missing:
            return f"Unknown photo_ids: {missing}", 404
        outside = [pid for pid in photo_ids if not db.workspace_folders.photo_in_workspace(pid, db.require_workspace_id())]
        if outside:
            return (
                f"Photo IDs do not belong to the active workspace: {outside}",
                403,
            )
        return None

    def _parse_photo_preference_body(body: JsonDict, require_photo: bool = True) -> tuple[JsonDict, str | None]:
        """``(parsed, None)``, or ``({}, message)``."""
        purpose = body.get("purpose", "")
        species = body.get("species", "")
        purpose = purpose.strip() if isinstance(purpose, str) else ""
        species = species.strip() if isinstance(species, str) else ""
        if purpose not in {"species_representative", "life_list", "highlights"}:
            return {}, (
                "purpose must be one of species_representative, "
                "life_list, or highlights"
            )
        if not species:
            return {}, "species required"
        # Canonicalize to the spelling add_keyword would store (see
        # _parse_species_highlight_body) so the eligibility prechecks and
        # the DB setters all key on one string.
        species = get_db().resolve_species_display_name(species)
        if not species:
            return {}, "species required"

        photo_id = body.get("photo_id")
        # Ordered highlights are per-photo (multiple photos per species),
        # so photo_id is always required for purpose=highlights — even on
        # DELETE, where representative purposes let it be omitted to clear
        # the whole species preference.
        needs_photo = require_photo or purpose == "highlights"
        if needs_photo:
            if isinstance(photo_id, bool) or not isinstance(photo_id, int):
                return {}, "photo_id must be an integer"
        else:
            photo_id = None

        return {
            "purpose": purpose,
            "canonical_purpose": (
                "highlights" if purpose == "highlights"
                else "species_representative"
            ),
            "species": species,
            "photo_id": photo_id,
        }, None

    def _photo_can_be_life_list_preference(db: Database, species: str, photo_id: int) -> bool:
        # Accept a hierarchy leaf whose taxon links back to a root
        # identification with the same curation name. See the matching
        # comment on SpeciesCurationRepository.get_representative_lists: after
        # root-repair the photo may only carry the hierarchical leaf
        # ("verdin"), whose spelling differs from the root ("Verdin") even
        # though the same taxon is still attached. Without this fallback,
        # updating a preserved root-key preference would fail eligibility.
        # Eligibility mirrors :meth:`SpeciesCurationRepository.get_life_list_candidates` and
        # `species_curation.get_photo_life_list_species` — both admit linked higher-rank
        # taxonomy identifications so a photo tagged only with a genus /
        # family / class entry can save the life-list representative for
        # the entry it actually renders under. Ancestor suppression is
        # applied via the shared clause so the write is denied for an
        # ancestor keyword (``Aves``) when the same photo carries a
        # descendant identification (``American Robin``); saving
        # ``Aves`` there would render as ``is_current_photo`` false on
        # the next read (see :meth:`species_curation.get_representative_lists`)
        # and leave a curation row nothing else considers eligible.
        return db.species_curation.photo_is_life_list_preference_eligible(species, photo_id)

    def _photo_can_be_highlights_preference(db: Database, species: str, photo_id: int) -> bool:
        candidates = db.species_curation.get_highlights_candidates(None, min_quality=0.0)
        buckets, _unidentified = collect_highlight_buckets(
            candidates, confidence_threshold=0.0,
            canonicalize_species=species_canonicalizer(db),
        )
        for bucket in buckets:
            if bucket["species"] != species:
                continue
            return any(p["id"] == photo_id for p in bucket["photos"])
        return False

    def _photo_can_be_preference(db: Database, purpose: str, species: str, photo_id: int) -> bool:
        # Ordered highlights use their own (stricter) eligibility check —
        # the photo must appear in the workspace's highlight buckets for
        # this species. Representative-style purposes (species_representative,
        # life_list) only require the photo to carry the species keyword.
        if purpose == "highlights":
            return _photo_can_be_highlights_preference(db, species, photo_id)
        return _photo_can_be_life_list_preference(db, species, photo_id)

    @blueprint.route("/api/photo-preferences", methods=["POST"])
    def api_photo_preferences_set() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        parsed, error = _parse_photo_preference_body(body)
        if error:
            return json_error(error)
        invalid = _validate_highlight_photo_ids(db, [parsed["photo_id"]])
        if invalid:
            return json_error(*invalid)
        if not _photo_can_be_preference(
            db, parsed["purpose"], parsed["species"], parsed["photo_id"]
        ):
            return json_error(
                "photo_id is not eligible for that purpose/species", 400,
            )
        # Route legacy `purpose=highlights` writes to the ordered-highlights
        # table so a cached Highlights page or older client doesn't silently
        # overwrite the species representative instead.
        if parsed["purpose"] == "highlights":
            with db.transaction():
                rank = db.species_curation.add_highlight(
                    parsed["species"], parsed["photo_id"]
                )
            return jsonify({"ok": True, **parsed, "rank": rank})
        # The representative write and its promotion land together.
        with db.transaction():
            db.set_species_representative(
                parsed["species"], parsed["photo_id"]
            )
            # Only promote to `species_highlights` when the photo is actually
            # a Highlights candidate. Otherwise the row is invisible on the
            # Highlights page (which filters by quality_score) — the user
            # could neither see it nor remove it.
            rank = None
            if _photo_can_be_highlights_preference(
                db, parsed["species"], parsed["photo_id"]
            ):
                rank = db.species_curation.promote_highlight(
                    parsed["species"], parsed["photo_id"]
                )
        return jsonify({"ok": True, **parsed, "highlight_rank": rank})

    @blueprint.route("/api/photo-preferences", methods=["DELETE"])
    def api_photo_preferences_clear() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        parsed, error = _parse_photo_preference_body(body, require_photo=False)
        if error:
            return json_error(error)
        # See the POST handler: `purpose=highlights` targets one row in the
        # ordered-highlights table, so it needs a photo_id (enforced by
        # _parse_photo_preference_body) and routes to species_curation.remove_highlight
        # rather than clearing all representative rows for the species.
        if parsed["purpose"] == "highlights":
            with db.transaction():
                removed = db.species_curation.remove_highlight(
                    parsed["species"], parsed["photo_id"]
                )
            return jsonify({
                "ok": True,
                "purpose": parsed["canonical_purpose"],
                "species": parsed["species"],
                "photo_id": parsed["photo_id"],
                "removed": removed,
            })
        with db.transaction():
            db.species_curation.clear_representative(parsed["species"])
        return jsonify({
            "ok": True,
            "purpose": parsed["canonical_purpose"],
            "species": parsed["species"],
        })

    def _parse_species_highlight_body(body: JsonDict, require_direction: bool = False) -> tuple[JsonDict, str | None]:
        """``(parsed, None)``, or ``({}, message)``."""
        species = body.get("species", "")
        species = species.strip() if isinstance(species, str) else ""
        if not species:
            return {}, "species required"
        # Canonicalize to the spelling add_keyword would store: the client
        # sends bucket labels, which for unconfirmed buckets derive from
        # prediction casing. The eligibility precheck compares against
        # canonical bucket keys and the DB setters canonicalize on write,
        # so parse-time canonicalization keeps all three in agreement.
        species = get_db().resolve_species_display_name(species)
        if not species:
            return {}, "species required"
        photo_id = body.get("photo_id")
        if isinstance(photo_id, bool) or not isinstance(photo_id, int):
            return {}, "photo_id must be an integer"
        parsed = {"species": species, "photo_id": photo_id}
        if require_direction:
            direction = body.get("direction", "")
            direction = direction.strip().lower() if isinstance(direction, str) else ""
            if direction not in {"up", "down"}:
                return {}, "direction must be up or down"
            parsed["direction"] = direction
        return parsed, None

    @blueprint.route("/api/species-highlights", methods=["POST"])
    def api_species_highlights_add() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        parsed, error = _parse_species_highlight_body(body)
        if error:
            return json_error(error)
        invalid = _validate_highlight_photo_ids(db, [parsed["photo_id"]])
        if invalid:
            return json_error(*invalid)
        if not _photo_can_be_highlights_preference(
            db, parsed["species"], parsed["photo_id"]
        ):
            return json_error(
                "photo_id is not eligible as a highlight for that species", 400,
            )
        with db.transaction():
            rank = db.species_curation.add_highlight(parsed["species"], parsed["photo_id"])
        return jsonify({"ok": True, **parsed, "rank": rank})

    @blueprint.route("/api/species-highlights", methods=["DELETE"])
    def api_species_highlights_remove() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        parsed, error = _parse_species_highlight_body(body)
        if error:
            return json_error(error)
        invalid = _validate_highlight_photo_ids(db, [parsed["photo_id"]])
        if invalid:
            return json_error(*invalid)
        with db.transaction():
            removed = db.species_curation.remove_highlight(parsed["species"], parsed["photo_id"])
        return jsonify({"ok": True, **parsed, "removed": removed})

    @blueprint.route("/api/species-highlights/order", methods=["PATCH", "POST"])
    def api_species_highlights_order() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        parsed, error = _parse_species_highlight_body(body, require_direction=True)
        if error:
            return json_error(error)
        invalid = _validate_highlight_photo_ids(db, [parsed["photo_id"]])
        if invalid:
            return json_error(*invalid)
        with db.transaction():
            ok = db.species_curation.move_highlight(
                parsed["species"], parsed["photo_id"], parsed["direction"]
            )
        if not ok:
            return json_error("highlight not found", 404)
        return jsonify({"ok": True, **parsed})

    @blueprint.route("/api/highlights")
    def api_highlights() -> ResponseReturnValue:
        db = get_db()
        payload = build_highlights_payload(db, HighlightsQuery(
            scope=request.args.get("scope", "folder"),
            folder_id=request.args.get("folder_id", type=int),
            min_quality=request.args.get("min_quality", 0.0, type=float),
            confidence_threshold=request.args.get(
                "confidence_threshold", 0.70, type=float
            ),
            limit_per_bucket=request.args.get("limit_per_bucket", 20, type=int),
            species=TextMatch(
                request.args.get("species") or "",
                match_case=request_bool_arg("species_match_case"),
                whole_word=request_bool_arg("species_whole_word"),
            ),
            search=TextMatch(
                request.args.get("q") or "",
                match_case=request_bool_arg("q_match_case"),
                whole_word=request_bool_arg("q_whole_word"),
            ),
            confirmation_filter=request.args.get("confirmation") or "all",
            highlight_filter=request.args.get("highlight_selection") or "all",
            representative_filter=(
                request.args.get("species_representative") or "all"
            ),
        ))
        return jsonify(payload)

    @blueprint.route("/api/highlights/confirm", methods=["POST"])
    def api_highlights_confirm() -> ResponseReturnValue:
        """Confirm highlight photos by accepting a prediction on each.

        With ``species`` (the bucket the user confirmed from), each photo's
        prediction for that species is accepted, so a photo already tagged
        with another species gains this one. Without it, each photo's top
        prediction is accepted and photos carrying any species are skipped.

        A prediction-decision route, so it takes the shared writer lock: it
        reads which photos already carry a species and which prediction is top
        *before* it accepts, and those reads have to be atomic with the
        accepts for the same reason the batch endpoints' are.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids, error = _parse_highlight_photo_ids(body)
        if error:
            return json_error(error)
        species, error = _parse_optional_species(body, "species")
        if error:
            return json_error(error)
        invalid = _validate_highlight_photo_ids(db, photo_ids)
        if invalid:
            return json_error(*invalid)
        return prediction_decisions.under_prediction_decision_lock(
            db, lambda: _highlights_confirm_under_lock(db, photo_ids, species),
            json_error=json_error,
        )

    def _highlights_confirm_under_lock(db: Database, photo_ids: list[int], species: str | None) -> Response:
        if species is None:
            targets, skips = _top_prediction_confirm_targets(db, photo_ids)
        else:
            targets, skips = _species_confirm_targets(db, photo_ids, species)
        affected, skipped = _accept_confirm_targets(
            db, photo_ids, targets, skips, exact_rows=species is not None,
        )
        db.after_commit(db._prune_edit_history)
        return jsonify({"ok": True, "affected": affected, "skipped": skipped})

    @blueprint.route("/api/highlights/relabel", methods=["POST"])
    def api_highlights_relabel() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids, error = _parse_highlight_photo_ids(body)
        if error:
            return json_error(error)
        species = body.get("species", "")
        species = species.strip() if isinstance(species, str) else ""
        # Normalize up front: the curation snapshots below compare this
        # value against stored species strings, and add_keyword would
        # normalize on insert anyway — one spelling throughout the route.
        species = normalize_keyword_display(species)
        if not species:
            return json_error("species required")
        # Canonicalize the submitted spelling to the final stored keyword
        # name BEFORE the snapshot passes below. resolve_species_display_name
        # mirrors add_keyword's two branches: preserve an existing NOCASE
        # match, otherwise apply the species casing convention (`black
        # phoebe` → `Black Phoebe`). Without this, the `row["species"] ==
        # species` snapshots (hl_dst_preexisting, pref_dst_taken,
        # rep_dst_preexisting, plus the source-side skips in the
        # preference/rep passes) would miss pre-existing target rows for
        # the destination species — from another keyword row, or from
        # prediction-seeded curation with no keyword row — and record
        # dst_existed=false; undo would then delete the user's
        # pre-existing highlight/preference/rep.
        species = db.resolve_species_display_name(species)
        from_species, error = _parse_optional_species(body, "from_species")
        if error:
            return json_error(error)
        # Multi-homonym reconciliation. resolve_species_display_name
        # preserves the caller's spelling when two intentionally-distinct
        # root species keywords share a NOCASE key (legacy general
        # ``Robin`` alongside taxonomy ``robin``) so bucket / parse /
        # setter paths stay in agreement on one string. add_keyword's
        # typed lookup below still prefers the taxonomy row, however, so
        # a request submitted as ``Robin`` would snapshot dst_existed
        # against ``Robin`` and then rename onto ``robin`` — leaving undo
        # able to delete pre-existing ``robin`` curation rows it never
        # created. Mirror add_keyword's ORDER BY here so snapshots and
        # renames key on the exact spelling add_keyword will store. Rows
        # with type NOT IN ('taxonomy', 'general') are excluded (matching
        # add_keyword) so a deliberate individual/location/genre homonym
        # doesn't misroute the target.
        target = db.keywords.top_level_species_keyword(species)
        if target and target["name"]:
            species = target["name"]
        invalid = _validate_highlight_photo_ids(db, photo_ids)
        if invalid:
            return json_error(*invalid)
        # Relabelling rejects each photo's top prediction, so it is a
        # prediction-decision route and takes the shared writer lock. Taken
        # here, before ``get_top_unrejected_predictions_by_photo`` — the read
        # that picks *which* row gets rejected is exactly the read that must
        # not race a concurrent decision on that row.
        return prediction_decisions.under_prediction_decision_lock(
            db, lambda: _highlights_relabel_under_lock(
                db, photo_ids, species, from_species,
            ),
            json_error=json_error,
        )

    def _highlights_relabel_under_lock(db: Database, photo_ids: list[int], species: str, from_species: str | None) -> Response:
        relabel = _HighlightsRelabel(
            db, photo_ids, species,
            _relabel_source_predictions(db, photo_ids, from_species),
            from_species=from_species,
        )
        relabel.snapshot_curation()
        kid = db.add_keyword(species, is_species=True)
        relabel.adopt_stored_spelling(kid)
        relabel.retag_photos(kid)
        relabel.rename_curation()
        relabel.record_relabel_edit(kid)
        db.after_commit(db._prune_edit_history)
        return jsonify({
            "ok": True,
            "keyword_id": kid,
            # The canonical stored spelling (after resolve_species_display_name
            # and add_keyword's casing convention). The lightbox retargets its
            # active bucket to this exact string, so a request submitted as
            # ``black phoebe`` points the still-open controls at the stored
            # ``Black Phoebe`` bucket rather than falling back to another
            # bucket of a multi-species photo.
            "species": relabel.species,
            "affected": relabel.items,
            "rejected_prediction_ids": relabel.rejected_prediction_ids,
        })

    @blueprint.route("/api/highlights/bucket")
    def api_highlights_bucket() -> ResponseReturnValue:
        db = get_db()
        folders = db.folders.with_quality_data()
        scope = request.args.get("scope", "folder")
        folder_id = request.args.get("folder_id", type=int)
        if scope == "workspace":
            folder_id = None
        elif folder_id is None and folders:
            folder_id = folders[0]["id"]

        min_quality = request.args.get("min_quality", 0.0, type=float)
        confidence_threshold = request.args.get(
            "confidence_threshold", 0.70, type=float
        )
        confirmation_filter = normalize_highlight_confirmation_filter(
            request.args.get("confirmation") or "all"
        )
        species = (request.args.get("species") or "").strip()
        offset = max(0, request.args.get("offset", 0, type=int))
        limit = max(1, min(request.args.get("limit", 100, type=int), 500))
        if not species:
            return json_error("species required")

        candidates = db.species_curation.get_highlights_candidates(folder_id, min_quality=min_quality)
        buckets, unidentified_photos = collect_highlight_buckets(
            candidates, confidence_threshold, confirmation_filter,
            canonicalize_species=species_canonicalizer(db),
        )
        buckets, unidentified_photos = filter_highlight_sections(
            buckets,
            unidentified_photos,
            request.args.get("q") or "",
            request_bool_arg("q_match_case"),
            request_bool_arg("q_whole_word"),
        )
        apply_ordered_highlights(db, buckets)
        apply_highlight_preferences(db, buckets)
        buckets, unidentified_photos = filter_highlight_curation_state(
            buckets,
            unidentified_photos,
            request.args.get("highlight_selection") or "all",
            request.args.get("species_representative") or "all",
        )
        if species == "__unidentified__":
            photos = unidentified_photos
            label = "Unidentified"
        else:
            bucket = next((b for b in buckets if b["species"] == species), None)
            if bucket is None:
                return json_error("species not found", 404)
            photos = bucket["photos"]
            label = bucket["species"]

        chunk = photos[offset: offset + limit]
        attach_edit_recipes(db, chunk)
        # Include the full-bucket ordering keys so a client refetch of a
        # paged bucket (has_more still true after the loaded window) can
        # keep bucket.best_score / best_timestamp anchored to the actual
        # tail. Recomputing them from only the loaded slice would drop
        # a large species below its true Recommended/Best sort position
        # when the highest-scored photo lives past the loaded window.
        top = photos[0] if photos else {}
        return jsonify({
            "species": label,
            "photos": chunk,
            "photo_count": len(photos),
            "loaded_count": min(len(photos), offset + len(chunk)),
            "has_more": offset + len(chunk) < len(photos),
            "best_score": bucket_best_score(photos),
            "best_timestamp": top.get("timestamp") if top else None,
        })

    @blueprint.route("/api/highlights/save", methods=["POST"])
    def api_highlights_save() -> ResponseReturnValue:
        db = get_db()

        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        name = body.get("name", "").strip()

        if not isinstance(photo_ids, list) or not photo_ids:
            return json_error("photo_ids required")
        # A nested or non-int entry is stored verbatim and later makes the
        # collection's rules unresolvable (every read 400s), so validate the
        # shape here, and keep ids to the photos this workspace can see.
        if any(isinstance(pid, bool) or not isinstance(pid, int) for pid in photo_ids):
            return json_error("photo_ids must be a list of integers")
        photo_ids = list(dict.fromkeys(photo_ids))
        visible_ids = set(db.photo_visibility.visible_photo_ids(photo_ids))
        foreign = [pid for pid in photo_ids if pid not in visible_ids]
        if foreign:
            return json_error(
                f"photo_ids not in the active workspace: {foreign}", 403
            )
        if not name:
            return json_error("name required")

        rules = json.dumps([{"field": "photo_ids", "value": photo_ids}])
        with db.transaction():
            cid = db.collections.add(name, rules)
        return jsonify({"ok": True, "id": cid})

    return blueprint
