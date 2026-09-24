"""Browse: the photo grid's first paint, summary, filter bar and selection panel.

* ``/api/browse/init`` is the combined first-paint payload (photos, folder
  tree, keywords, collections, folder-health snapshot) read from one SQLite
  snapshot, and ``/api/browse/summary`` the scoped summary strip.
* ``/api/filters/*`` backs the filter bar: the field registry, the configured
  quick-filter shortcuts, and typeahead values with live counts.
* ``/api/selection/*`` aggregates keywords, pending predictions and wildlife
  state across a multi-photo selection.
* ``/api/browse`` (GET), ``/api/browse/photo-counts`` and ``/api/browse/mkdir``
  are the folder picker's filesystem listing, per-folder photo counts and
  directory creation.
"""

from __future__ import annotations

import json
import os

import filter_shortcuts
from filter_fields import SUGGEST_FIELDS, fields_for_api
from flask import Blueprint, current_app, jsonify, request
from photo_payload import prepare_browse_photo_dicts
from services.prediction_ambiguity import ambiguous_prediction_ids
from services.visual_scope import collection_rules_state, inject_active_visual_model
from web.collections import _collection_accepts_manual_photos
from web.request_args import (
    MAX_PER_PAGE,
    parse_selection_photo_ids,
    reject_visual_collection,
    request_bool_arg,
    request_rules_arg,
    request_visual_arg,
)


def create_browse_blueprint(
    get_db,
    json_error,
    *,
    visual_scope,
):
    """Build the browse, filter-bar and selection blueprint.

    ``visual_scope`` is the app's one ``VisualScope``: it owns the per-app
    query-text embedding cache, so the summary and typeahead routes must share
    ``create_app``'s instance rather than build their own. ``MAX_PER_PAGE``
    is the page-size cap shared with the ``/api/photos`` listing routes.
    ``services.prediction_ambiguity.ambiguous_prediction_ids`` is the one
    definition of "a bare Accept must not act on this prediction";
    ``batch-accept`` re-derives the same verdict under the prediction-decision
    lock, and the selection panel's split must match it.
    """
    blueprint = Blueprint("browse", __name__)

    # -- Browse grid --

    @blueprint.route("/api/browse/init")
    def api_browse_init():
        """Combined endpoint for browse page initial load — one request instead of five."""
        import config as cfg
        db = get_db()
        page = max(1, request.args.get("page", 1, type=int))
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), MAX_PER_PAGE))
        sort = request.args.get("sort", "date")
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        focus_photo_id = request.args.get("focus_photo_id", None, type=int)
        # A focused deep link needs the requested photo itself to be a grid
        # item. If it is a hidden member of a stack, stack pagination cannot
        # express its position, so focused loads deliberately use ordinary
        # photo rows; the user can re-enable stacks after the handoff.
        stacks = request_bool_arg("stacks") and focus_photo_id is None

        # Keep the combined first-paint payload on one SQLite read snapshot.
        # Independent SELECT snapshots can straddle a background folder-health
        # commit and combine pre-transition photos/missing IDs with a
        # post-transition folder tree. Since the navbar poll may already have
        # completed, there is no guaranteed immediate observation to repair
        # that split response.
        db.conn.execute("BEGIN")

        # ``db.get_photos(collection_id=...)`` / ``count_filtered_photos`` both
        # expand only ``collections.rules`` — the same rules-only path guarded
        # elsewhere by ``reject_visual_collection``. A visual-only collection
        # stores ``rules: []``, so running it through that path here would
        # silently widen first paint to every workspace photo. The client's
        # ``bootstrapBrowse()`` calls ``filterByCollection()`` right after,
        # which reloads through ``/api/photos/query`` with the visual clause,
        # but the wrong grid still flashes between the two — and any failure
        # before ``filterByCollection()`` runs leaves the user staring at the
        # widened scope. Drop the ``collection_id`` from the photo query and
        # return an empty grid so the wrong data can never appear (Codex
        # review on PR #1343). Folders/keywords/collections are still returned
        # so the sidebar bootstraps.
        visual_first_paint = False
        if collection_id is not None:
            coll_row = db.conn.execute(
                "SELECT visual_json FROM collections "
                "WHERE id = ? AND workspace_id = ?",
                (collection_id, db._ws_id()),
            ).fetchone()
            if coll_row is not None and coll_row["visual_json"] is not None:
                visual_first_paint = True

        # Snapshot the active workspace's missing-folder IDs so the client's
        # navbar can seed ``_missingFoldersLastIds`` from this init response.
        # Without a baseline, the first /api/folders/missing observation
        # returns false when a background ``MissingOriginals.folder_health_loop`` flip runs
        # between init and the poll — later polls then see the same IDs and
        # never dispatch, leaving Browse stuck showing the pre-flip state
        # (Codex review r3686191141).
        #
        # This is the first read in the explicit transaction above, so every
        # photo / folder / keyword / collection read below shares the same
        # health snapshot. Ordering alone is insufficient: if the background
        # health loop commits after this query but before get_folder_tree(),
        # and the navbar's initial poll already finished, no immediate poll
        # remains to repair the mixed response (Codex reviews r3686317681 and
        # r3687501744).
        missing_folder_ids = [f["id"] for f in db.get_missing_folders()]
        folder_health_version = db.get_folder_health_version()

        # First paint is scope-only (folder / collection / sort); metadata
        # filter deep links compile into the filter bar client-side, which
        # reloads through /api/photos/query once initialized (Phase 5 —
        # legacy per-field params removed from this endpoint).
        if visual_first_paint:
            photos = []
            total = 0
            underlying_total = 0
            stack_count = 0
            focus_index = None
            focus_page = page
        else:
            try:
                if stacks:
                    stack_cfg = db.browse_stack_settings(cfg.load())
                    photos = db.query_browse_stacks(
                        [], folder_id=folder_id, collection_id=collection_id,
                        page=page, per_page=per_page, sort=sort,
                        stack_config=stack_cfg,
                    )
                    stack_totals = db.browse_stack_totals(
                        [], folder_id=folder_id, collection_id=collection_id,
                        stack_config=stack_cfg,
                    )
                    total = stack_totals["total"]
                    stack_count = stack_totals["stack_count"]
                    underlying_total = db.count_photos_for_rules(
                        [], folder_id=folder_id, collection_id=collection_id,
                    )
                else:
                    photos = db.get_photos(
                        folder_id=folder_id,
                        collection_id=collection_id,
                        page=page,
                        per_page=per_page,
                        sort=sort,
                    )
            except ValueError as exc:
                db.conn.rollback()
                return json_error(str(exc), 400)
            if not stacks:
                if not any([folder_id, collection_id]):
                    total = db.count_photos()
                else:
                    try:
                        total = db.count_filtered_photos(
                            folder_id=folder_id,
                            collection_id=collection_id,
                        )
                    except ValueError as exc:
                        db.conn.rollback()
                        return json_error(str(exc), 400)
                underlying_total = total
            # Photo deep links need the target's position in the exact sort
            # order used by this first paint. Returning the zero-based index
            # and its bounded page from this read snapshot avoids probing
            # serial 50-photo pages until the target happens to appear. Keep
            # the extra ordered-id query opt-in so ordinary Browse loads retain
            # their existing cost.
            #
            # When the target is already in the first-page ``photos`` fetched
            # above (the common case — Highlights and most native actions open
            # a photo whose sorted position is on page 1), compute the index
            # from that page directly.  Materializing every folder ID would
            # add O(folder size) latency and memory to the critical first
            # paint on the documented 100K- and 1M-photo libraries, even when
            # a lookup was never necessary (Codex review r3792637103).
            focus_index = None
            focus_page = page
            if focus_photo_id is not None:
                local_index = None
                for offset, row in enumerate(photos):
                    if row["id"] == focus_photo_id:
                        local_index = offset
                        break
                if local_index is not None:
                    focus_index = (page - 1) * per_page + local_index
                else:
                    try:
                        focus_index = db.get_photo_position(
                            focus_photo_id,
                            folder_id=folder_id,
                            collection_id=collection_id,
                            sort=sort,
                        )
                    except ValueError as exc:
                        db.conn.rollback()
                        return json_error(str(exc), 400)
                # Return only the bounded page containing the target from the
                # same SQLite read snapshot as its position and total. Sending
                # every preceding row can freeze Browse for a focus deep in a
                # million-photo folder; independent parallel offset requests,
                # meanwhile, allow ingestion/deletion to create overlaps,
                # gaps, and stale totals. One target page avoids both failure
                # modes, and ordinary lazy paging resumes after it.
                if focus_index is not None:
                    focus_page = focus_index // per_page + 1
                    if focus_page != page:
                        try:
                            photos = db.get_photos(
                                folder_id=folder_id,
                                collection_id=collection_id,
                                page=focus_page,
                                per_page=per_page,
                                sort=sort,
                            )
                        except ValueError as exc:
                            db.conn.rollback()
                            return json_error(str(exc), 400)
        folders = db.get_folder_tree()
        keywords = db.get_keyword_tree()
        collections = db.get_collections()

        photo_dicts = prepare_browse_photo_dicts(db, photos)
        collection_dicts = []
        for c in collections:
            d = dict(c)
            # Flag degraded rules at first paint so a fast click on the
            # sidebar row can't fall into the 400 /photos path before
            # loadCollectionCounts() lands. Only validate the rules — do
            # not run COUNT(DISTINCT p.id) per collection here; that
            # N+1 is what the async loadCollectionCounts() call in
            # bootstrapBrowse() was designed to avoid, and re-adding it
            # to the critical first-paint path makes opening Browse wait
            # on every smart-collection query. Malformed JSON is treated
            # as degraded too — those rules would 400 downstream just
            # like an unresolvable rule.
            parsed_rules, degraded = collection_rules_state(db, c["rules"])
            if degraded:
                d["can_add_photos"] = False
                d["count_error"] = True
                collection_dicts.append(d)
                current_app.logger.warning(
                    "Collection %s (%s) has unresolvable rules; marking degraded",
                    c["id"],
                    c["name"],
                )
                continue
            else:
                # A visual-only collection stores ``rules: []`` which
                # ``_collection_accepts_manual_photos`` treats as addable,
                # but ``/add-photos`` would just append to ``photo_ids``
                # without touching ``visual_json`` — the manually added
                # photos would only show up in the reopened collection
                # if they also matched the hidden visual clause, making
                # the add silently ineffective (Codex r3620791304).
                d["can_add_photos"] = (
                    c["visual_json"] is None
                    and _collection_accepts_manual_photos(parsed_rules)
                )
            collection_dicts.append(d)

        response_payload = {
            "photos": photo_dicts,
            "total": total,
            "page": page,
            "per_page": per_page,
            "folders": [dict(f) for f in folders],
            "keywords": [dict(k) for k in keywords],
            "collections": collection_dicts,
            "missing_folder_ids": missing_folder_ids,
            "folder_health_version": folder_health_version,
            "focus_index": focus_index,
            "focus_page": focus_page,
            # The workspace this tree was scoped to. Browse pins it to
            # the destructive folder-removal call so a cross-tab
            # workspace switch between render and click cannot redirect
            # the DELETE at another workspace.
            "active_workspace_id": db._ws_id(),
        }
        if stacks:
            response_payload["underlying_total"] = underlying_total
            response_payload["stack_count"] = stack_count
        response = jsonify(response_payload)
        # End the read transaction after every value in the response has been
        # materialized. rollback() is intentional: this endpoint is read-only
        # and it releases the snapshot without implying a write commit.
        db.conn.rollback()
        return response

    @blueprint.route("/api/browse/summary")
    def api_browse_summary():
        db = get_db()
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        # ``get_browse_summary`` narrows through ``_build_collection_query``,
        # which reads only ``collections.rules``. Without ``visual``, a saved
        # visual collection would silently widen the summary to every
        # metadata match (Codex review r3622521597). Browse itself clears
        # ``activeCollectionId`` when opening a collection into the filter
        # bar, so its ``/api/browse/summary`` calls never carry a visual
        # collection id; this guards direct API callers.
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        try:
            rules = request_rules_arg()
            visual = request_visual_arg()
            rules, _visual_info = visual_scope.apply_to_rules(
                db, rules, visual,
                collection_id=collection_id, folder_id=folder_id,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        try:
            summary = db.get_browse_summary(
                folder_id=folder_id,
                collection_id=collection_id,
                rules=rules,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        return jsonify(summary)

    @blueprint.route("/api/filters/fields")
    def api_filter_fields():
        """The filter field registry: one source of truth for the UI picker
        (labels, categories, types, operators, enum values, suggest flags).
        """
        return jsonify({"fields": fields_for_api()})

    @blueprint.route("/api/filters/shortcuts")
    def api_filter_shortcuts():
        """The configured quick-filter buttons for the filter bar's row.

        ``shortcuts`` is the stored list with the behavior the bar needs
        derived from each rule's shape (see filter_shortcuts.py); ``groups``
        is the same list split into the containers it renders; ``defaults``
        backs the Settings "restore the built-in buttons" action, so both
        sides read one definition of what the built-ins are.
        """
        import config as cfg

        shortcuts = filter_shortcuts.from_config(cfg.load())
        return jsonify({
            "shortcuts": shortcuts,
            "groups": filter_shortcuts.grouped(shortcuts),
            "defaults": filter_shortcuts.DEFAULT_SHORTCUTS,
        })

    @blueprint.route("/api/filters/values")
    def api_filter_values():
        """Typeahead values with live counts for a suggest-capable field.

        ``rules`` (JSON, optional) is the active expression minus the rule
        being edited, so counts answer "how many results would I get" under
        the user's other selections — never a global COUNT(*).
        ``folder_id``/``collection_id`` (optional) narrow the count set to
        the page-scope restriction Browse applies to ``/api/photos/query``,
        so a folder- or dashboard-collection-scoped Browse view shows
        typeahead counts that match its visible grid instead of the whole
        workspace.
        """
        db = get_db()
        field = request.args.get("field", "")
        if field not in SUGGEST_FIELDS:
            return json_error(f"field {field!r} does not support value suggestions", 400)
        raw_rules = request.args.get("rules")
        rules = []
        if raw_rules:
            try:
                rules = json.loads(raw_rules)
            except ValueError:
                return json_error("rules must be valid JSON", 400)
        q = request.args.get("q") or None
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        # ``get_filter_field_values`` narrows counts through
        # ``_build_collection_query``, which reads only ``collections.rules``.
        # Without ``visual``, a saved visual collection would silently widen
        # the typeahead counts to every metadata match (Codex review
        # r3622521597).
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        # Clamp ``limit`` to a positive bounded range. Werkzeug's ``type=int``
        # cheerfully parses ``?limit=0``/``?limit=-5`` (SQLite treats a
        # negative ``LIMIT`` as "no limit" and returns everything), and
        # ``?limit=999999`` on a large library would load an unbounded
        # suggestions list. Fallback to the 20-default when parsing fails.
        limit_raw = request.args.get("limit", 20, type=int)
        if limit_raw is None:
            limit_raw = 20
        limit = max(1, min(limit_raw, 500))
        rules = inject_active_visual_model(rules)
        try:
            visual = request_visual_arg()
            rules, _visual_info = visual_scope.apply_to_rules(
                db, rules, visual,
                collection_id=collection_id, folder_id=folder_id,
            )
        except ValueError as exc:
            return json_error(str(exc), 400)
        try:
            values = db.get_filter_field_values(
                field, rules=rules, q=q, limit=limit,
                folder_id=folder_id, collection_id=collection_id,
            )
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify({"field": field, "values": values})

    # -- Selection panel --

    @blueprint.route("/api/selection/keyword-suggestions", methods=["POST"])
    def api_selection_keyword_suggestions():
        """Return selected keywords and which selected photos carry each one."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
        if err is not None:
            return err

        rows = []
        batch_size = 800
        for i in range(0, len(photo_ids), batch_size):
            chunk = photo_ids[i:i + batch_size]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(db.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.type
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE pk.photo_id IN ({placeholders})
                    ORDER BY LOWER(k.name), k.id""",
                chunk,
            ).fetchall())

        selected_count = len(photo_ids)
        by_keyword = {}
        for row in rows:
            entry = by_keyword.setdefault(
                row["id"],
                {
                    "id": row["id"],
                    "name": row["name"],
                    "type": row["type"],
                    "photo_ids": set(),
                },
            )
            entry["photo_ids"].add(row["photo_id"])

        keywords = []
        for entry in by_keyword.values():
            count = len(entry["photo_ids"])
            missing_count = selected_count - count
            present_photo_ids = [
                pid for pid in photo_ids if pid in entry["photo_ids"]
            ]
            missing_photo_ids = [
                pid for pid in photo_ids if pid not in entry["photo_ids"]
            ]
            item = {
                "id": entry["id"],
                "name": entry["name"],
                "type": entry["type"],
                "count": count,
                "missing_count": missing_count,
                "present_photo_ids": present_photo_ids,
                "missing_photo_ids": missing_photo_ids,
            }
            keywords.append(item)
        keywords.sort(
            key=lambda item: (-item["count"], item["name"].lower(), item["id"])
        )
        suggestions = [item for item in keywords if 0 < item["count"] < selected_count]
        return jsonify({
            "selected_count": selected_count,
            "keywords": keywords,
            "suggestions": suggestions,
        })

    @blueprint.route("/api/selection/prediction-suggestions", methods=["POST"])
    def api_selection_prediction_suggestions():
        """Aggregate pending predictions across a selection, grouped by species.

        The keyword panel next to this one answers "what have I already
        tagged"; this answers "what does the classifier think I should tag".
        Aggregation is by species match key rather than prediction id because
        each selected photo carries its own prediction row.

        Predictions the Browse panel must not offer a bare Accept for —
        those with an alternative sibling, or a disagreement/refinement
        against existing keywords — are returned separately in
        ``ambiguous_prediction_ids`` so the button can name the count it
        will actually act on. ``CORE_PHILOSOPHY.md`` forbids an "Accept on
        38" that quietly accepts 35.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
        if err is not None:
            return err

        # Apply the workspace's confidence floor, exactly as the single-photo
        # panel does. Without it a real catalog fills this list with 2%-and-up
        # noise wearing an actionable "Accept on 1" button. The hidden count
        # is reported rather than dropped.
        import config as cfg
        threshold = db.get_effective_config(cfg.load()).get(
            "classifier_confidence", 0.0
        ) or 0.0
        all_pending = db.get_predictions(photo_ids=photo_ids, status="pending")
        pending = [
            row for row in all_pending if (row["confidence"] or 0.0) >= threshold
        ]
        below_threshold_count = len(all_pending) - len(pending)
        # Which of these rows a bare Accept must not touch — an alternative
        # sibling, or a disagreement/refinement against the photo's *current*
        # species keywords. Computed by the same helper ``batch-accept``
        # re-runs before writing, so the button's promise and the endpoint's
        # precondition cannot describe different sets.
        ambiguous_row_ids = ambiguous_prediction_ids(db, pending)

        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        order = {pid: i for i, pid in enumerate(photo_ids)}
        by_key = {}
        for row in pending:
            # Group by the species the accept path will actually apply, not
            # the raw per-frame label: for a burst whose frames disagree,
            # ``accept_prediction`` uses the individual-vote consensus, so a
            # Robin/Robin/Sparrow burst must aggregate under Robin — the
            # Sparrow minority frame would otherwise surface its own bucket
            # advertising an Accept button that tags Robin.
            identity = resolver.consensus(row)
            species = identity.display_name
            if not species:
                continue
            key = identity.key
            entry = by_key.setdefault(key, {
                "species": species,
                "models": set(),
                "rows_by_photo": {},
                "confidences": [],
            })
            entry["models"].add(row["model"])
            # ``row["confidence"]`` is the classifier's confidence in the
            # row's own per-frame label. For a burst whose consensus differs
            # from that label — a Sparrow-labelled frame in a Robin-majority
            # burst — the raw score reflects Sparrow evidence, not Robin,
            # and pooling it under Robin would rank the bucket at Sparrow's
            # 95% and let the earlier threshold filter keep Robin actionable
            # on the strength of a vote for someone else. Only credit
            # confidence when the raw label agrees with the consensus.
            raw_species = row["species"]
            row_matches_consensus = bool(
                raw_species
                and resolver.prediction(row).key == key
            )
            if row_matches_consensus:
                entry["confidences"].append(row["confidence"] or 0.0)
            ambiguous = row["id"] in ambiguous_row_ids
            # One photo can hold several detections of the same species, or
            # the same detection classified by several models. Keep every
            # matching prediction id so the batch accept flips them all —
            # keywording the photo once and leaving nothing pending in Review.
            # Submitting only the top-confidence id used to accept that row,
            # keyword the photo, and then hide the sibling rows behind the
            # "already keyworded" filter with no way to resolve them.
            photo_entry = entry["rows_by_photo"].setdefault(row["photo_id"], {
                "ids": [],
                "max_confidence": None,
                "ambiguous": False,
            })
            photo_entry["ids"].append((row["confidence"] or 0, row["id"]))
            if ambiguous:
                photo_entry["ambiguous"] = True
            if row_matches_consensus:
                conf = row["confidence"] or 0
                if (
                    photo_entry["max_confidence"] is None
                    or conf > photo_entry["max_confidence"]
                ):
                    photo_entry["max_confidence"] = conf

        # Resolve "already carries this species" against linked taxa, not
        # spelling, so a hierarchical Birds|Verdin tag counts as keyworded.
        # Look up existing rows only — asking here must not create keywords.
        kid_by_key = {}
        for row in db.conn.execute(
            "SELECT k.id, k.name, COALESCE(k.source_taxon_id, t.inat_id) AS source_id, "
            "t.name AS scientific_name FROM keywords k LEFT JOIN taxa t ON t.id = k.taxon_id "
            "WHERE k.is_species = 1 OR k.type = 'taxonomy'"
        ).fetchall():
            source = {"taxon_id": row["source_id"], "scientific_name": row["scientific_name"]} if row["source_id"] else None
            identity = resolver.resolve(row["name"], row["scientific_name"], source)
            kid_by_key.setdefault(identity.key, row["id"])

        results = []
        # A bucket-level suppression used to sit here: when the threshold was
        # active and ``entry["confidences"]`` came out empty, the bucket was
        # dropped and counted into a ``suppressed_borrowed_count`` the panel
        # rendered as its own empty state. Both are gone, because the state
        # they described is gone. ``confidences`` can only be empty when
        # *every* contributing row's raw label disagrees with the consensus
        # the accept path applies, and that is precisely the legacy shape
        # ``Database.repair_mixed_species_prediction_groups`` clears at
        # startup: after the repair a grouped row's consensus is its own
        # species, so every surviving row credits its own bucket. Keeping a
        # filter plus a second piece of empty-state vocabulary for an
        # unreachable case is its own transparency cost — the user would be
        # asked to learn a distinction the data can no longer produce.
        #
        # The credit gate above stays. It is four lines, it computes the
        # right number rather than hiding a row, and if the invariant it
        # rests on ever regressed it degrades to "confidence unknown"
        # instead of quoting one species' score beside another's name.
        for key, entry in by_key.items():
            predicted_ids = sorted(entry["rows_by_photo"], key=lambda p: order[p])
            kid = kid_by_key.get(key)
            keyworded = (
                db.get_photos_with_equivalent_species(predicted_ids, kid)
                if kid is not None else set()
            )
            missing_ids = [p for p in predicted_ids if p not in keyworded]
            # A pending prediction on an already-keyworded photo still has to
            # go somewhere: dropping it would leave Review holding the same
            # row the panel just told the user was "already keyworded", and
            # the user has no way to clear it from Browse. ``accept_prediction``
            # already supports a status-only accept — ``add_keyword`` is
            # idempotent and the ``already_has_species`` branch skips the
            # re-tag — so an unambiguous keyworded row is safe to include
            # here. Ambiguous keyworded rows still route to Review, same as
            # ambiguous missing ones, because their resolution needs Review's
            # full comparison UI.
            acceptable, ambiguous_ids, ambiguous_photos = [], [], []
            acceptable_photos = []
            for pid in predicted_ids:
                row = entry["rows_by_photo"][pid]
                # Highest confidence first so a stable, obvious order lands in
                # the payload (and any downstream truncation keeps the
                # strongest evidence per photo).
                ids_here = [
                    rid for _c, rid in sorted(row["ids"], reverse=True)
                ]
                if row["ambiguous"]:
                    ambiguous_ids.extend(ids_here)
                    ambiguous_photos.append(pid)
                else:
                    acceptable.extend(ids_here)
                    acceptable_photos.append(pid)
            # Photo-level count for the button label. A photo with several
            # matching detections contributes several ids to ``acceptable``, so
            # ``len(acceptable)`` overcounts the photos that would be touched.
            acceptable_photo_count = len(acceptable_photos)
            already_keyworded_acceptable = sum(
                1 for pid in acceptable_photos if pid in keyworded
            )
            results.append({
                "species": entry["species"],
                "models": sorted(entry["models"]),
                "predicted_count": len(predicted_ids),
                # Count of photos already carrying this species keyword. Kept
                # informational — the panel labels them so the user knows
                # some of the "Accept on N" targets will status-only flip
                # without re-tagging.
                "keyworded_count": len(predicted_ids) - len(missing_ids),
                # Subset of ``acceptable_photo_count`` whose keyword is
                # already present. Lets the UI say "already keyworded on M
                # of N" without recomputing the intersection client-side.
                "acceptable_keyworded_count": already_keyworded_acceptable,
                "predicted_photo_ids": predicted_ids,
                "missing_photo_ids": missing_ids,
                "prediction_ids": acceptable + ambiguous_ids,
                "acceptable_prediction_ids": acceptable,
                "acceptable_photo_ids": acceptable_photos,
                "acceptable_photo_count": acceptable_photo_count,
                "ambiguous_prediction_ids": ambiguous_ids,
                "ambiguous_photo_ids": ambiguous_photos,
                # ``confidences`` can be empty when every credit-eligible row
                # was a minority frame whose raw label disagreed with the
                # consensus (see above). ``None`` renders as
                # ``formatPredictionConfidence(null)`` → "confidence unknown"
                # rather than inventing a 0% or misattributing a minority
                # frame's score to the consensus species.
                "min_confidence": (
                    min(entry["confidences"]) if entry["confidences"] else None
                ),
                "max_confidence": (
                    max(entry["confidences"]) if entry["confidences"] else None
                ),
            })
        # Breadth first, then strength: the panel collapses to the top few, so
        # a species predicted on every selected photo at 100% must outrank one
        # predicted on the same photos at 20%.
        results.sort(key=lambda item: (
            -item["predicted_count"],
            -(item["max_confidence"] or 0.0),
            item["species"].lower(),
        ))
        return jsonify({
            "selected_count": len(photo_ids),
            "predictions": results,
            "threshold": threshold,
            "below_threshold_count": below_threshold_count,
        })

    @blueprint.route("/api/selection/wildlife-state", methods=["POST"])
    def api_selection_wildlife_state():
        """Aggregate wildlife_excluded state across the full selection.

        The browse panel may hold IDs that are not yet loaded into the
        client-side grid (e.g. after "Select all matching"), so the counts
        must be derived server-side or the batch include/exclude controls
        would silently omit off-page photos and hide the buttons that
        should still be available for them.

        Any submitted IDs that are missing or outside the active workspace
        are reported as ``missing_count`` so the panel can surface an
        incomplete selection instead of implying the counts cover
        everything the user picked. Without that signal the state endpoint
        and the batch endpoint disagree on what "the selection" means and
        the panel can show actions for a subset that then fail when
        applied to the full list.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list):
            return json_error("photo_ids required")

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers")
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return jsonify({
                "requested_count": 0,
                "selected_count": 0,
                "included_count": 0,
                "excluded_count": 0,
                "missing_count": 0,
            })

        ws_id = db._ws_id()
        included = 0
        excluded = 0
        batch_size = 800
        for i in range(0, len(photo_ids), batch_size):
            chunk = photo_ids[i:i + batch_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT COALESCE(p.wildlife_excluded, 0) AS excluded
                    FROM photos p
                    WHERE p.id IN ({placeholders})
                      AND EXISTS (
                          SELECT 1 FROM workspace_folders wf
                          WHERE wf.folder_id = p.folder_id
                            AND wf.workspace_id = ?
                      )""",
                [*chunk, ws_id],
            ).fetchall()
            for row in rows:
                if row["excluded"]:
                    excluded += 1
                else:
                    included += 1

        accessible = included + excluded
        return jsonify({
            "requested_count": len(photo_ids),
            "selected_count": accessible,
            "included_count": included,
            "excluded_count": excluded,
            "missing_count": len(photo_ids) - accessible,
        })

    # -- Folder picker --

    @blueprint.route("/api/browse", methods=["GET"])
    def api_browse():
        """List subdirectories at a given path for folder browser."""
        from image_loader import is_excluded_scan_path
        path = request.args.get("path", os.path.expanduser("~"))
        # Reject macOS app-managed library bundles before any stat:
        # ``os.path.isdir`` on a ``.photoslibrary`` path — or a symlink to one
        # — itself trips the "access data from other apps" TCC prompt, and the
        # folder picker hits this listing endpoint before posting children to
        # ``/api/browse/photo-counts``, so the per-child guard there is too
        # late on its own.
        if is_excluded_scan_path(path):
            return json_error("path is not a valid directory")
        if not os.path.isdir(path):
            return json_error("path is not a valid directory")
        dirs = []
        try:
            for name in sorted(os.listdir(path), key=str.casefold):
                if name.startswith("."):
                    continue
                full = os.path.join(path, name)
                # Skip excluded bundles (and symlinks resolving to them)
                # before ``os.path.isdir`` would stat them.
                if is_excluded_scan_path(full):
                    continue
                if os.path.isdir(full):
                    dirs.append({"name": name, "path": full})
        except PermissionError:
            return json_error("permission denied", 403)
        return jsonify({"path": path, "dirs": dirs})

    @blueprint.route("/api/browse/photo-counts", methods=["POST"])
    def api_browse_photo_counts():
        """Return recursive photo-file counts for a list of folder paths.

        Used by the folder browser to show per-folder counts next to each
        subfolder so users can see which folders contain photos before
        selecting one.
        """
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        file_types = body.get("file_types", [])
        if not isinstance(paths, list):
            return json_error("paths must be a list", 400)

        from image_loader import is_excluded_scan_path
        from ingest import discover_source_files

        ft = file_types if file_types else "both"
        counts = {}
        for p in paths:
            # Non-string entries (dicts, lists, numbers) can't be dict keys
            # and aren't valid paths — skip them rather than 500.
            if not isinstance(p, str):
                continue
            # Reject macOS app-managed library bundles before any stat:
            # ``os.path.isdir`` on a ``.photoslibrary`` path — or a symlink to
            # one — itself trips the "access data from other apps" TCC prompt,
            # and the folder browser sends every child of ``~/Pictures`` here
            # the moment the picker opens.
            if is_excluded_scan_path(p):
                counts[p] = 0
                continue
            if not os.path.isdir(p):
                counts[p] = 0
                continue
            try:
                discovered = discover_source_files(p, file_types=ft, recursive=True)
                counts[p] = len(discovered)
            except (OSError, PermissionError):
                counts[p] = 0
        return jsonify({"counts": counts})

    @blueprint.route("/api/browse/mkdir", methods=["POST"])
    def api_browse_mkdir():
        """Create a new directory."""
        body = request.get_json(silent=True) or {}
        path = body.get("path", "")
        if not path:
            return json_error("path is required")
        if not os.path.isabs(path):
            return json_error("path must be absolute")
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            return json_error(str(e), 500)
        return jsonify({"name": os.path.basename(path), "path": path})

    return blueprint
