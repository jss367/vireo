"""Prediction review: listing, Compare, and the accept/reject decisions.

* ``/api/predictions`` lists predictions (optionally scoped to a collection,
  rules, a visual search or explicit ``photo_ids``) with nested alternatives
  and the current-keyword ambiguity verdict; ``/api/predictions/compare`` is
  the side-by-side Compare payload; ``/api/predictions/group/<id>`` reads one
  burst group.
* The decision routes -- ``<id>/accept``, ``<id>/accept-subject``,
  ``<id>/reject``, ``<id>/reviewed``, ``<id>/replace-keywords``,
  ``batch-accept``, ``batch-reject`` and ``group/apply`` -- write
  ``prediction_review`` state, so each takes the shared prediction-decision
  lock through ``services.prediction_decisions`` (see
  ``PREDICTION_DECISION_ROUTES`` there; the view names are kept unchanged so
  that set still matches).
"""

from __future__ import annotations

import json

import id_conflicts
from db import Database
from flask import Blueprint, jsonify, request
from keyword_normalization import keyword_match_key
from photo_payload import (
    attach_nested_edit_recipes,
    attach_species_representatives,
    render_key_for_recipe,
)
from services import prediction_decisions
from services.pending_changes import queue_keyword_add
from services.prediction_ambiguity import ambiguous_prediction_ids, effective_category_resolver, prediction_is_ambiguous
from services.visual_scope import inject_active_visual_model
from sql_chunks import chunked
from web.request_args import (
    MAX_SELECTION_PHOTOS,
    parse_selection_photo_ids,
    reject_visual_collection,
    request_rules_arg,
    request_visual_arg,
)


def _request_photo_ids_arg():
    """Parse the optional ``photo_ids`` query param (comma-separated ints).

    Returns None when absent so callers keep their unscoped behaviour, and
    raises ValueError on a malformed value so a typo surfaces as a 400
    rather than silently widening the query to the whole workspace.

    Capped at the same 1000 ids ``parse_selection_photo_ids`` allows:
    ``/api/predictions`` runs one ``_photo_in_workspace`` query per id, so
    an unbounded list turns a single GET into unbounded database work.
    """
    raw = request.args.get("photo_ids")
    if raw is None or raw.strip() == "":
        return None
    ids = []
    seen = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            pid = int(part)
        except ValueError as exc:
            raise ValueError("photo_ids must be integers") from exc
        if pid not in seen:
            ids.append(pid)
            seen.add(pid)
    if not ids:
        raise ValueError("photo_ids must contain at least one id")
    if len(ids) > 1000:
        raise ValueError("too many photo_ids")
    return ids


def create_predictions_blueprint(
    get_db,
    json_error,
    config,
    *,
    visual_scope,
):
    """Build the prediction listing, Compare and review-decision blueprint.

    ``config`` is ``app.config``, read at request time for Compare's
    ``ID_CONFLICTS_SNAPSHOTS`` store. ``visual_scope`` is the app's one
    ``VisualScope`` (it owns the per-app query-text embedding cache), so the
    listing shares ``create_app``'s instance rather than building its own.

    ``services.prediction_ambiguity`` is the one definition of "a bare Accept
    must not act on this prediction"; the browse blueprint's selection panel
    uses the same ``ambiguous_prediction_ids`` so its split matches what
    ``batch-accept`` re-derives under the lock here.
    """
    blueprint = Blueprint("predictions", __name__)

    @blueprint.route("/api/predictions")
    def api_predictions():
        db = get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        status = request.args.get("status", None)
        # Browse's detail panel asks for one photo's predictions. Reusing this
        # route rather than adding a per-photo one keeps a single definition of
        # "a prediction" — latest-fingerprint dedup, nested alternatives and
        # ``existing_species`` enrichment all come along, so the Browse panel
        # can never disagree with Review about what is pending.
        try:
            explicit_photo_ids = _request_photo_ids_arg()
        except ValueError as e:
            return json_error(str(e), 400)
        if explicit_photo_ids is not None:
            for pid in explicit_photo_ids:
                if not db._photo_in_workspace(pid):
                    return json_error(
                        f"Photo {pid} does not belong to the active workspace",
                        403,
                    )
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        try:
            rules = request_rules_arg()
            visual = request_visual_arg()
            # Fill in the active visual model on any UI-emitted
            # ``has_visual_index`` rule that omits it. /api/photos/query
            # already injects here; the Review GET path must do the same
            # so a workspace with stale embeddings from an inactive model
            # doesn't include predictions for photos that aren't indexed
            # by the currently-active visual model.
            rules = inject_active_visual_model(rules)
            # Forward ``collection_id`` so ``visual_info`` (matched /
            # candidates / indexed) describes the collection-scoped Review
            # queue the filter bar chip is showing — not a workspace-wide
            # proxy that misrepresents the actual queue and also wastes
            # embedding work on photos outside it.
            rules, visual_info = visual_scope.apply_to_rules(
                db, rules, visual, collection_id=collection_id,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        try:
            if collection_id:
                photos = db.get_collection_photos(collection_id, per_page=999999)
                photo_ids = [p["id"] for p in photos]
                # An explicit ``photo_ids`` narrows the collection rather than
                # replacing it, so a Browse panel opened inside a collection
                # can't surface a photo the collection excludes.
                if explicit_photo_ids is not None:
                    allowed = set(photo_ids)
                    photo_ids = [
                        pid for pid in explicit_photo_ids if pid in allowed
                    ]
                preds = (
                    db.get_predictions(photo_ids=photo_ids, status=status, rules=rules)
                    if photo_ids
                    else []
                )
            else:
                preds = db.get_predictions(
                    photo_ids=explicit_photo_ids, status=status, rules=rules,
                )

            # Fetch alternatives to attach to their parent predictions.
            # Constrain by the returned parents' photo_ids and skip ``rules``
            # for this lookup: row-level parent predicates (e.g.
            # ``prediction_confidence >= 0.8`` or
            # ``prediction_status is pending``) evaluate against each row's
            # own values, so alternatives — whose status is
            # ``alternative`` and whose confidence/species usually differ
            # from the matching parent — would otherwise be dropped by
            # ``_filter_prediction_rows_by_rules`` before ``alts_by_key`` is
            # built. The parent would then render with an empty
            # ``alternatives`` list and the user could not accept an
            # alternate species in that filtered view. The
            # ``(detection_id, model)`` key in ``alts_by_key`` already
            # restricts attachment to alternatives whose parent is in
            # ``preds``, so no extra rows leak into the response.
            alt_preds = []
            if not status or status == "pending":
                parent_photo_ids = list({
                    p["photo_id"] for p in preds
                    if p["photo_id"] is not None
                })
                if parent_photo_ids:
                    alt_preds = db.get_predictions(
                        photo_ids=parent_photo_ids, status="alternative",
                    )
        except ValueError as e:
            return json_error(str(e), 400)

        # Index alternatives by (detection_id, model)
        alts_by_key = {}
        for a in alt_preds:
            ad = dict(a)
            key = (ad["detection_id"], ad["model"])
            alts_by_key.setdefault(key, []).append({
                "id": ad["id"],
                "species": ad["species"],
                "confidence": ad["confidence"],
                "taxonomy_kingdom": ad.get("taxonomy_kingdom"),
                "taxonomy_phylum": ad.get("taxonomy_phylum"),
                "taxonomy_class": ad.get("taxonomy_class"),
                "taxonomy_order": ad.get("taxonomy_order"),
                "taxonomy_family": ad.get("taxonomy_family"),
                "taxonomy_genus": ad.get("taxonomy_genus"),
                "scientific_name": ad.get("scientific_name"),
            })

        # Enrich predictions and attach alternatives
        results = []
        pred_dicts = [dict(p) for p in preds]
        attach_species_representatives(db, pred_dicts)
        recipes_by_photo = db.get_photo_edit_recipes({
            p.get("photo_id") for p in pred_dicts if p.get("photo_id") is not None
        })
        # Same recomputation as the selection aggregator: the stored
        # ``category`` is a classify-time snapshot, so a keyword added
        # after classification leaves it stale. Compare each pending
        # prediction against the CURRENT species keywords on its photo and
        # expose the fresh disposition as ``effective_category`` so
        # Browse's single-photo panel can route now-conflicting predictions
        # to Review instead of offering a bare Accept.
        pending_photo_ids = {
            d.get("photo_id") for d in pred_dicts
            if d.get("status") != "alternative" and d.get("photo_id") is not None
        }
        effective_category_of = effective_category_resolver(
            db, pending_photo_ids,
        )
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        for d in pred_dicts:
            if d.get("status") == "alternative":
                continue  # alternatives are nested, not top-level
            d["edit_recipe"] = recipes_by_photo.get(d.get("photo_id"))
            d["render_key"] = render_key_for_recipe(d["edit_recipe"])
            # Species the accept path will actually apply. For an ordinary
            # prediction this is the row's own species; for a grouped/burst
            # prediction whose frames disagree, ``accept_prediction`` derives
            # the burst consensus from ``individual`` vote counts. The Browse
            # panel labels and groups rows by this so a Sparrow frame in a
            # majority-Robin burst never surfaces a Sparrow row whose Accept
            # actually tags Robin. Computed before the comparison below
            # because that species is the one that would land on the photo.
            identity = resolver.consensus(d)
            d["consensus_species"] = identity.display_name
            d["consensus_species_key"] = identity.key
            d["species_key"] = resolver.prediction(d).key
            effective_category = (
                effective_category_of(
                    d.get("photo_id"), d.get("consensus_species"), identity,
                )
                if effective_category_of is not None else None
            )
            d["effective_category"] = effective_category
            if prediction_is_ambiguous(effective_category, d.get("category")):
                keywords = db.get_photo_keywords(d["photo_id"])
                d["existing_species"] = [
                    k["name"] for k in keywords
                    if db.is_keyword_species(k["id"])
                ]
            # Attach alternatives
            key = (d.get("detection_id"), d.get("model"))
            d["alternatives"] = alts_by_key.get(key, [])
            results.append(d)
        # Surface the visual clause's status so the Review filter bar's
        # visual chip can warn on fallback. Without this the chip would
        # advertise a visual search while Accept All / bulk operations ran
        # over the broadened metadata-only prediction set. Response is a
        # dict envelope so the field can travel alongside the list;
        # callers unwrap ``data.predictions``.
        response = {"predictions": results}
        if visual_info is not None:
            response["visual"] = visual_info
        # Only the per-photo callers (Browse's detail panel) need this, and
        # only they can afford it — Review asks for the whole workspace queue.
        # Without it an empty list is ambiguous: "never classified" and
        # "classified, found nothing" would render identically.
        if explicit_photo_ids is not None:
            response["photo_states"] = {
                str(pid): state
                for pid, state in db.get_prediction_states(explicit_photo_ids).items()
            }
            # Whether the best label in the list actually matched, per photo.
            # A prediction row carries a softmax confidence, which is a
            # ranking within the list and cannot distinguish "this is a chat"
            # from "nothing here is a chat, but this is closest". Sent with the
            # same per-photo gating as photo_states: Browse's panel shows one
            # photo, Review asks for the entire queue and must not pay for it.
            import config as cfg
            import match_confidence
            effective_cfg = db.get_effective_config(cfg.load())
            response["match_states"] = {
                str(pid): match_confidence.summarize_photo(
                    db.get_match_scores_for_photo(pid),
                    effective_cfg,
                    unscored_current_runs=(
                        db.get_unscored_current_prediction_runs(pid)
                    ),
                )
                for pid in explicit_photo_ids
            }
        return jsonify(response)

    _COMPARE_MAX_PER_PAGE = 200

    def _compare_photo_ids(name="photo_id"):
        """Parse repeated photo id args, or ``(None, None)`` when absent."""
        values = request.args.getlist(name)
        if not values:
            return None, None
        if len(values) > MAX_SELECTION_PHOTOS:
            return None, json_error(f"too many {name}s")
        try:
            photo_ids = [int(value) for value in values]
        except (TypeError, ValueError):
            return None, json_error(f"{name} must be an integer")
        if any(photo_id <= 0 for photo_id in photo_ids):
            return None, json_error(f"{name} must be a positive integer")
        return photo_ids, None

    def _attach_comparison_render_keys(db, photos):
        """Give ID Conflicts rows the fingerprint their thumbnail URLs need.

        The page builds ``/thumbnails/<id>.jpg`` through ``vireoThumbnailUrl``
        like every other grid, and that URL only carries an ``er`` fingerprint
        when the photo dict does. Thumbnails answer ``Cache-Control: public,
        max-age=86400``, so without one a browser that cached a row's
        thumbnail before an edit keeps showing the pre-edit image for a day.

        These rows are keyed by ``photo_id`` rather than ``id``, so they
        cannot go through ``attach_edit_recipes``.
        """
        if not photos:
            return photos
        recipes = db.get_photo_edit_recipes(
            [photo["photo_id"] for photo in photos],
        )
        for photo in photos:
            recipe = recipes.get(photo["photo_id"])
            photo["edit_recipe"] = recipe
            photo["render_key"] = render_key_for_recipe(recipe)
        return photos

    @blueprint.route("/api/predictions/compare")
    def api_predictions_compare():
        """One page of the ID Conflicts comparison, plus every count it shows.

        Deriving what a row means — its status, whether the models disagree,
        whether it still needs review — takes a full pass over the
        collection's predictions and the taxonomy, so the result is kept as a
        snapshot (see id_conflicts) and the browser is handed a token to come
        back with. Filtering, sorting, searching and paging then run against
        that snapshot instead of re-deriving anything, and the rows on screen
        are rebuilt from the database so what the user acts on is current.

        Passing ``photo_id`` instead returns just those rows, with no
        snapshot: that is how a decision refreshes what it changed.
        """
        db = get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        if not collection_id:
            return json_error("collection_id required")
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        requested_photo_ids, err = _compare_photo_ids()
        if err is not None:
            return err

        requested_models = request.args.getlist("model")
        min_confidence = request.args.get("min_confidence", type=float)
        if min_confidence is None:
            min_confidence = id_conflicts.DEFAULT_MIN_CONFIDENCE
        min_confidence = min(1.0, max(0.0, min_confidence))

        if requested_photo_ids is not None:
            built = id_conflicts.build_comparison(
                db, collection_id, photo_ids=requested_photo_ids,
            )
            visible = id_conflicts.resolve_models(
                built["models"], requested_models,
            )
            for photo in built["photos"]:
                id_conflicts.attach_assessment(photo, visible, min_confidence)
            _attach_comparison_render_keys(db, built["photos"])
            return jsonify(built)

        refresh_ids, err = _compare_photo_ids("refresh_photo_id")
        if err is not None:
            return err

        workspace_id = db._ws_id()
        store = config["ID_CONFLICTS_SNAPSHOTS"]
        snapshot = store.get(request.args.get("token"))
        if snapshot is not None and not snapshot.matches(
            collection_id, workspace_id,
            id_conflicts.resolve_models(snapshot.all_models, requested_models),
            min_confidence,
        ):
            # The page changed something the snapshot was derived under —
            # collection, workspace, shown models or the conflict threshold —
            # so it has to be derived again.
            snapshot = None
        # A grouped decision can pull in a sibling carrying a model nothing in
        # the snapshot had. The rows, the columns and every count in there
        # were derived without it, so the snapshot is spent: serving from it
        # would put a column on the page whose numbers never considered the
        # model naming it. Derive the comparison again now, on this request.
        # That makes one decision click pay for a full rebuild, which on a
        # catalog-sized collection is seconds — but it takes a sibling
        # carrying a model no other photo in the collection has, and the
        # alternative is answering with numbers we know are wrong.
        if (
            snapshot is not None
            and refresh_ids
            and snapshot.patch(db, refresh_ids)
        ):
            store.discard(snapshot.token)
            snapshot = None
        if snapshot is None:
            snapshot = store.put(id_conflicts.build_snapshot(
                db, collection_id, workspace_id,
                models=requested_models, min_confidence=min_confidence,
            ))

        per_page = request.args.get("per_page", 60, type=int) or 60
        per_page = min(_COMPARE_MAX_PER_PAGE, max(1, per_page))
        selection = id_conflicts.select(
            snapshot.records,
            snapshot.models,
            filter_id=request.args.get("filter", "all"),
            excludes=request.args.getlist("exclude"),
            query=request.args.get("q", ""),
            match_case=request.args.get("match_case") == "1",
            whole_word=request.args.get("whole_word") == "1",
            sort=request.args.get("sort", "review_priority"),
            page=request.args.get("page", 1, type=int) or 1,
            per_page=per_page,
        )
        return jsonify({
            "token": snapshot.token,
            "models": snapshot.all_models,
            "visible_models": snapshot.models,
            "taxonomy_available": snapshot.taxonomy_available,
            "photos": _attach_comparison_render_keys(db, id_conflicts.page_rows(
                db, collection_id, selection.photo_ids,
                snapshot.models, snapshot.min_confidence,
            )),
            "page": selection.page,
            "per_page": per_page,
            "total": selection.total,
            "summary": selection.summary,
            "filter_counts": selection.filter_counts,
            "exclusion_counts": selection.exclusion_counts,
            "filters": [
                {"id": fid, "label": label} for fid, label in id_conflicts.FILTERS
            ],
            "sorts": [
                {"id": sid, "label": label} for sid, label in id_conflicts.SORTS
            ],
            "excludes": [
                {"id": eid, "label": label} for eid, label in id_conflicts.EXCLUDES
            ],
        })

    @blueprint.route("/api/predictions/<int:pred_id>/reviewed", methods=["POST"])
    def api_mark_prediction_reviewed(pred_id):
        """Mark a pending prediction as reviewed, atomically.

        Under the same lock as the accept/reject routes. The existing
        pending-only precondition was correct but was read outside a
        transaction, so a batch-accept that landed between the read and the
        write could accept the row and then the reviewed write would overwrite
        the batch's ``accepted`` status with ``reviewed`` — losing the
        accept and the keyword together. Holding the writer lock across the
        read and the write is what makes the precondition actually mean what
        it says.
        """
        db = get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, d.photo_id,
                          COALESCE(pr_rev.status, 'pending') AS status
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.id = ?""",
                (db._ws_id(), pred_id),
            ).fetchone()
            if pred is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            # Only pending predictions may transition to reviewed. Without
            # this guard a stale/double request or a direct API call against
            # an already accepted/rejected prediction would silently
            # overwrite the prior decision, corrupting review state and
            # audit history.
            if pred["status"] != "pending":
                db.conn.rollback()
                return json_error(
                    f'prediction already {pred["status"]}; cannot mark reviewed',
                    409,
                )
            db.update_prediction_status(pred_id, "reviewed", _commit=False)
            db.record_edit(
                "prediction_reviewed",
                f'Marked prediction "{pred["species"]}" reviewed',
                "reviewed",
                [{
                    "photo_id": pred["photo_id"],
                    "old_value": "pending",
                    "new_value": "reviewed",
                }],
                _commit=False,
            )
            db.conn.commit()
            return jsonify({"ok": True})
        except Exception:
            db.conn.rollback()
            raise

    @blueprint.route("/api/predictions/<int:pred_id>/replace-keywords", methods=["POST"])
    def api_replace_species_keywords_with_prediction(pred_id):
        """Accept a prediction and replace existing species keywords atomically.

        Same lock as every other prediction-decision route. Without it, a
        replace-keywords request racing a batch-reject on the same row would
        strip conflicting species keywords under an accept while the reject
        commits the row as ``rejected`` — leaving the replaced photo tagged
        with a species now marked rejected, and the old species permanently
        lost even though undo cannot restore it.
        """
        db = get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            current_status = _prediction_status(db, pred_id)
            if current_status is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            # accept_prediction(replace_species=True) strips existing
            # species/taxonomy keywords from *every* photo it tags (the whole
            # group, not just this photo) inside one transaction, so grouped
            # photos are replaced consistently.
            result = db.accept_prediction(
                pred_id, replace_species=True, _commit=False,
            )
            if result is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            items = [
                {
                    "photo_id": a["photo_id"],
                    "old_value": ", ".join(a.get("old_species", [])),
                    "new_value": result["species"],
                }
                for a in result["affected"]
            ]
            is_batch = len(items) > 1
            desc = f'Replaced species keyword with "{result["species"]}"'
            if is_batch:
                desc += f" across {len(items)} photos"
            db.record_edit(
                "prediction_replace_species",
                desc,
                result["species"],
                items,
                is_batch=is_batch,
                _commit=False,
            )
            db.conn.commit()
            # Same reason as ``api_accept_prediction``: replace goes through
            # the same grouped expansion, so a looping caller needs the rows
            # this transaction decided rather than the one it asked about.
            return jsonify({
                "ok": True,
                "prediction_ids": result["accepted_prediction_ids"],
                "photo_ids": result["photo_ids"],
            })
        except Exception:
            db.conn.rollback()
            raise

    @blueprint.route("/api/predictions/batch-accept", methods=["POST"])
    def api_batch_accept_predictions():
        """Accept many predictions of one species as a single action.

        Browse's selection panel accepts a species across the whole selection.
        This goes through ``accept_prediction`` rather than
        ``/api/batch/keyword`` because a prediction has two halves: the
        keyword tag AND the ``prediction_review`` status. Tagging alone would
        leave every photo still pending in Review, so the same work would have
        to be done a second time there.

        The whole batch lands as ONE ``prediction_accept`` edit so a single
        undo reverses it, matching the per-photo ``changed_tag`` / ``no_tag``
        encoding ``api_accept_prediction`` already uses.

        With ``photo_ids`` and ``expected_species``, Browse's "Accept on all"
        also adds that species to every selected photo missing it. Existing
        keywords are preserved. Prediction decisions still follow the checks
        below; photos without an acceptable prediction get keyword-only undo
        items. ``accepted`` counts photos changed by either part of the action.

        Contract: every row this endpoint accepts is one that is still
        undecided, still unambiguous, and still from the current label set *at
        the moment of the write* — judged against the database rather than
        against whatever the caller's panel believed. All three preconditions
        are re-derived here: statuses via ``_decided_prediction_ids``,
        staleness via ``_superseded_prediction_ids``, ambiguity via
        ``ambiguous_prediction_ids`` (the same helper that produced the
        panel's ``acceptable_prediction_ids``). Rows failing any of them are
        skipped, never accepted, and counted back in ``already_decided``,
        ``skipped_superseded`` and ``skipped_ambiguous``. A stale payload can
        therefore accept less than the caller asked for, but never something
        the caller was not shown as acceptable.

        "At the moment of the write" is literal, not approximate: the checks
        and the writes run inside one ``BEGIN IMMEDIATE`` transaction
        (``prediction_decisions.begin_prediction_decision``), so no other
        connection can decide these rows in between. Two overlapping requests
        are serialized by SQLite's writer lock — the second reads what the
        first committed and skips accordingly, rather than acting on state it
        read before the first one wrote.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        # ``replace_species=True`` strips conflicting species keywords from
        # every tagged photo, but the batch endpoint records one
        # ``prediction_accept`` edit whose ``old_value`` carries only the
        # prediction id — no room for the removed keyword names. Undoing
        # that entry would restore prediction status but leave the
        # replaced species permanently gone. The single-photo replace
        # route (``/api/predictions/<id>/replace-keywords``) sidesteps this
        # by recording ``prediction_replace_species``, which is explicitly
        # non-undoable. Refuse the flag here rather than silently drop it
        # so a mis-wired caller learns immediately.
        if bool(body.get("replace_species")):
            return json_error(
                "replace_species is not supported on batch-accept because "
                "the batched undo entry cannot restore removed keywords; "
                "use /api/predictions/<pred_id>/replace-keywords for a "
                "single-photo replacement",
                400,
            )
        # An explicit selection extends the accept to photos without a
        # matching prediction. Their keyword additions share the same undo.
        all_photo_ids = None
        if "photo_ids" in body:
            all_photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
            if err is not None:
                return err
        if all_photo_ids is not None and body.get("prediction_ids") == []:
            pred_ids, err = [], None
        else:
            pred_ids, err = _parse_prediction_ids(db, body)
        if err is not None:
            return err

        # ``expected_species`` is the species the button in Browse names —
        # what "Accept on 38 Bald Eagle" would tag. Passed through so the
        # endpoint can refuse to accept a row whose grouping (and therefore
        # ``accept_prediction``'s applied species) drifted after the panel
        # rendered but before the lock. Optional so single-species callers
        # that already know they only submit one bucket at a time keep
        # working unchanged; when omitted, the drift check is skipped and
        # the endpoint's older contract holds.
        raw_expected = body.get("expected_species")
        expected_species = (
            raw_expected.strip() if isinstance(raw_expected, str) else None
        ) or None
        if all_photo_ids is not None and not expected_species:
            return json_error("expected_species required when accepting on all photos")

        # Everything from here to the commit is one transaction, taken with
        # the writer lock held from the first read (see
        # ``prediction_decisions.begin_prediction_decision``). The
        # preconditions below are only worth what their atomicity with the
        # write is worth: read them
        # outside the transaction and a second overlapping request can pass the
        # same checks against the same pre-write state.
        #
        # ``_parse_prediction_ids`` stays outside deliberately — it validates
        # the payload's shape and workspace ownership, which is not the state
        # these preconditions race against, and it can walk a 1,000-photo
        # selection. The lock is held for the decision, not for parsing. The
        # in-lock ``prediction_decisions.out_of_workspace_prediction_ids``
        # filter re-checks the workspace half so a folder detach that lands in
        # the window between parse and lock cannot tag a now-foreign photo.
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if all_photo_ids is not None:
                # Recheck ownership inside the write transaction, including
                # photos that have no prediction rows to validate below.
                all_photo_ids, err = parse_selection_photo_ids(db, body, json_error=json_error)
                if err is not None:
                    db.conn.rollback()
                    return err
                selected = set(all_photo_ids)
                if any(row["photo_id"] not in selected for row in _load_prediction_rows(db, pred_ids)):
                    db.conn.rollback()
                    return json_error("prediction_ids must belong to the selected photos")
            return _batch_accept_under_lock(db, pred_ids, expected_species, all_photo_ids)
        except Exception:
            db.conn.rollback()
            raise

    def _batch_accept_under_lock(db, pred_ids, expected_species=None, all_photo_ids=None):
        """The checks and writes of ``batch-accept``, inside its transaction.

        Split out only so the transaction's boundaries are impossible to
        misread: every statement here runs with the writer lock already held,
        and the single ``commit`` at the end is the moment any of it becomes
        visible to another request.
        """
        # Make a submission of an already-decided row a no-op rather than a
        # second accept. A double-clicked Accept button or a stale panel would
        # otherwise re-accept rows that are already accepted: the keyword now
        # exists, so the second pass records a status-only ``no_tag`` item
        # whose "previous" status is a fiction — undoing it would knock a
        # long-accepted prediction back to pending while keeping the keyword.
        # ``rejected`` rows are skipped for a sharper reason: when the user
        # accepts an alternative in Review or another tab, this row's sibling
        # wins and this row becomes the rejected loser. Accepting it from a
        # payload Browse rendered before that happened would tag the photo
        # with the species the user just rejected, and the batch's undo entry
        # would then reset the whole sibling scope to pending/alternative
        # instead of restoring the winner's accepted state.
        already_decided = _decided_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in already_decided]

        # Drop rows whose label set the catalog has moved past. Re-classifying
        # a detection after Browse rendered the panel leaves the old row
        # ``pending`` — nothing rewrites it — while every read path, including
        # the panel that produced this payload, has already switched to the
        # newest ``labels_fingerprint``. Accepting the old row would tag the
        # photo from a label set nothing displays and mark accepted a row the
        # user can no longer see, while the current prediction stayed pending.
        #
        # Before the ambiguity check so the two counts stay disjoint: a
        # superseded row is reported as superseded, not as a conflict the user
        # would go to Review to resolve and never find.
        superseded_ids = _superseded_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in superseded_ids]

        # Re-derive ambiguity instead of trusting the payload. The decided
        # filter above only catches rows whose *status* moved; a row can stay
        # ``pending`` and still stop being safe to bare-accept, because
        # ambiguity is a function of the photo's current species keywords.
        # Add a Golden Eagle keyword from Review or a second tab after Browse
        # rendered "Accept on 35", and the Bald Eagle row Browse listed as
        # acceptable is now a conflict — the panel would route it to Review,
        # but the button in the stale document still posts it. Skipping it
        # here is what makes "Accept" mean the same thing at click time as it
        # did at render time (``CORE_PHILOSOPHY.md``, no black boxes).
        #
        # Skipped rather than a 400 for the same reason ``already_decided``
        # is: the rest of the batch is still exactly what the user asked for,
        # and the panel refresh that follows re-renders the skipped rows in
        # their ambiguous form, with the Review hand-off. Failing the whole
        # call would strand 34 honest accepts on one row that moved.
        ambiguous_ids = ambiguous_prediction_ids(
            db, _load_prediction_rows(db, pred_ids),
        )
        pred_ids = [pid for pid in pred_ids if pid not in ambiguous_ids]

        # A folder detach — itself a write — can happen between
        # ``_parse_prediction_ids`` and ``BEGIN IMMEDIATE``. Re-check
        # workspace ownership here so a batch cannot tag a photo that left
        # the workspace in that window (and cannot write workspace-scoped
        # ``prediction_review`` state for a row it no longer owns). Skipped
        # rather than 403 for the same reason ``already_decided`` is: the
        # rest of the batch is still exactly what the user asked for, and
        # failing the whole call would strand honest accepts on one row that
        # moved.
        out_of_workspace_ids = (
            prediction_decisions.out_of_workspace_prediction_ids(db, pred_ids)
        )
        pred_ids = [pid for pid in pred_ids if pid not in out_of_workspace_ids]

        # The button in Browse names one species and the endpoint should
        # apply exactly that species. Consensus can drift after render:
        # another tab ungrouping the burst clears ``individual`` votes so
        # ``accept_prediction`` falls back to the raw per-frame label, and
        # per-vote edits can shift the winner. Skip rows whose current
        # consensus no longer matches. No-op when the caller passes no
        # species (older tests, single-species callers that never render a
        # multi-species button).
        drifted_ids = _species_drifted_prediction_ids(
            db, _load_prediction_rows(db, pred_ids), expected_species,
        )
        pred_ids = [pid for pid in pred_ids if pid not in drifted_ids]

        # Confine the whole batch to the rows the caller actually submitted.
        # ``accept_prediction`` otherwise expands a grouped (burst) accept to
        # every row in the group, tagging photos the user never selected.
        #
        # The limit travels as prediction ids, not as any photo set derived
        # from them, because a photo is not a unique key for a prediction row:
        # one burst photo can carry a row per classifier model and a row per
        # detection. Under a photo-set limit — batch-wide or per
        # ``(group, model)`` bucket — submitting photo A's model-X row and
        # photo B's model-Y row lets A's grouped accept reach B's model-X row,
        # a row the panel deliberately omitted (below threshold, ambiguous, or
        # already accepted). Row identity has no such projection to get wrong,
        # whatever column next distinguishes two rows on one photo. Note this
        # set is built *after* both filters above, so a row that was decided
        # elsewhere, or that has since become ambiguous, cannot be re-accepted
        # through a sibling's group expansion either.
        submitted_pred_ids = set(pred_ids)
        # A grouped accept resolves every submitted sibling in its group in
        # one call — including each one's losing alternatives, which
        # ``accept_prediction`` now rejects per accepted row rather than only
        # for the entry row. So the remaining siblings in this loop are
        # already fully done; re-entering them was O(N^2) in group size and
        # appended a second, status-only history item per photo whose
        # recorded "previous" status is a fiction.
        handled = set()
        items = []
        keyword_id = None
        species = None
        species_key = None
        for pid in pred_ids:
            if pid in handled:
                continue
            result = db.accept_prediction(
                pid,
                prediction_ids=submitted_pred_ids,
                _commit=False,
            )
            handled.add(pid)
            if result is None:
                continue
            accepted_now = result.get("accepted_prediction_ids", ())
            handled.update(accepted_now)
            if not accepted_now:
                # A no-op accept (nothing in this row's scope) changed
                # nothing, so it carries no species to reconcile against
                # the batch's — and its ``keyword_id`` may be None because
                # no keyword was created. Folding it into the check below
                # would 400 a perfectly uniform batch.
                continue
            # Browse groups predictions by species identity, not keyword ID.
            # Different aliases can legitimately tag different keyword rows
            # of that species. Record the actual ID on each history item so
            # undo/redo reverses exactly that tag, while still rejecting a
            # batch that resolves to genuinely different species.
            if keyword_id is None:
                keyword_id, species = result["keyword_id"], result["species"]
                species_key = result["species_key"]
            elif result["species_key"] != species_key:
                db.conn.rollback()
                return json_error(
                    "prediction_ids must all resolve to one species", 400,
                )
            for a in result["affected"]:
                if a.get("changed_tag", True):
                    old_value = str(a["prediction_id"])
                else:
                    old_value = json.dumps({
                        "prediction_id": a["prediction_id"],
                        "no_tag": True,
                    })
                items.append({
                    "photo_id": a["photo_id"],
                    "old_value": old_value,
                    "new_value": str(result["keyword_id"]),
                })

        has_accepted_predictions = bool(items)
        if all_photo_ids is not None:
            if keyword_id is None:
                from species_identity import SpeciesResolver
                resolver = SpeciesResolver(db=db)
                identity = resolver.display(expected_species)
                # Only bind the resolved taxon when ``expected_species``
                # carried an explicit ``(scientific)``/``(taxon N)``
                # qualifier. A bare common name is name-only inference,
                # and routing it through ``_add_source_species_keyword``
                # refuses to reuse an unlinked same-name keyword the async
                # ``mark_species_keywords`` pass has not touched yet,
                # minting a suffixed duplicate such as
                # ``California Towhee (taxon 42)``. Mirrors the guard in
                # ``accept_prediction`` so both accept paths behave the
                # same for a legacy/name-only bucket.
                explicit = resolver.explicit_source(expected_species) is not None
                keyword_id = db.add_keyword(
                    identity.display_name, is_species=True, _commit=False,
                    source_taxon_id=identity.taxon_id if explicit else None,
                )
                species = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (keyword_id,),
                ).fetchone()["name"]
            already_tagged = db.get_photos_with_equivalent_species(all_photo_ids, keyword_id)
            for photo_id in all_photo_ids:
                if photo_id in already_tagged:
                    continue
                db.tag_photo(photo_id, keyword_id, source="manual", _commit=False)
                items.append({
                    "photo_id": photo_id,
                    "old_value": json.dumps({"keyword_only": True}),
                    "new_value": str(keyword_id),
                })

            # Every new tag in Accept on all needs the same sidecar handling,
            # including tags already added by accept_prediction above.
            for item in items:
                old_value = item["old_value"]
                old_meta = (
                    json.loads(old_value) if old_value.startswith("{")
                    else {"prediction_id": int(old_value)}
                )
                if old_meta.get("no_tag"):
                    continue
                photo_id = item["photo_id"]
                item_species = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (int(item["new_value"]),),
                ).fetchone()["name"]
                flat_removals = [dict(row) for row in db.conn.execute(
                    """SELECT workspace_id, value FROM pending_changes
                       WHERE photo_id = ? AND change_type = 'keyword_remove_flat'
                         AND value = ? COLLATE NOCASE""",
                    (photo_id, item_species),
                )]
                # accept_prediction queues an add directly. Reconcile it
                # with any pending removal before applying the shared helper.
                db.remove_pending_changes(photo_id, "keyword_add", item_species, _commit=False)
                queue_keyword_add(db, photo_id, item_species, _commit=False)
                # Keep the suppression records cleared by the add, including
                # those in other workspaces sharing this photo's sidecar.
                old_meta.update(symmetric_keyword_queue=True, flat_removals=flat_removals)
                item["old_value"] = json.dumps(old_meta)

        # History joins the same transaction rather than committing after it:
        # the accepted statuses and the entry that undoes them become visible
        # together, so no reader can see accepted rows with no way back.
        if items:
            photo_count = len({item["photo_id"] for item in items})
            desc = (
                f'Accepted prediction: added "{species}"' if has_accepted_predictions
                else f'Added species "{species}"'
            )
            if photo_count > 1:
                desc += f" to {photo_count} photos"
            db.record_edit(
                "prediction_accept", desc, str(keyword_id), items,
                is_batch=photo_count > 1, _commit=False,
            )
        db.conn.commit()
        if items:
            # ``record_edit`` skips its prune under ``_commit=False``; run it
            # once the decision is durable so history stays bounded.
            db._prune_edit_history()
        return jsonify({
            "ok": True,
            "accepted": len({item["photo_id"] for item in items}),
            # Reported rather than folded into ``accepted``: a caller that
            # resubmits should be able to tell "nothing to do, already
            # decided" from "nothing matched". Named ``already_decided``
            # rather than ``already_accepted`` because it counts rejected
            # rows too — a field whose name implies a narrower set than it
            # holds is the kind of quiet mis-description CORE_PHILOSOPHY.md
            # rules out, and this count is the only signal a caller gets for
            # rows the batch deliberately left alone.
            "already_decided": len(already_decided),
            # Rows still pending but no longer safe to bare-accept, because
            # the photo's keywords moved after the panel rendered. Reported
            # separately from ``already_decided`` because the user's next step
            # differs: a decided row needs nothing, an ambiguous one needs
            # Review. Browse turns this into a toast rather than letting the
            # count vanish between "Accept on 35" and 33 accepts.
            "skipped_ambiguous": len(ambiguous_ids),
            # Rows still pending, but from a label set a later classification
            # run replaced. Its own count for the same reason: the user's next
            # step is neither "nothing" nor "Review" — the refreshed panel
            # simply shows the current prediction in this row's place, and a
            # count folded into ``skipped_ambiguous`` would send them hunting
            # for a keyword conflict that does not exist.
            "skipped_superseded": len(superseded_ids),
            # Rows whose photo left the workspace between parse and lock. A
            # panel refresh drops the photo from view, so the user's next
            # step is neither Review nor a re-run — just the refresh — and
            # folding it into any of the other counts would misname it.
            "skipped_out_of_workspace": len(out_of_workspace_ids),
            # Rows whose current consensus species no longer matches the one
            # the button named — another tab ungrouped the burst, or per-vote
            # edits shifted the winner. Only computed when the caller passed
            # ``expected_species``; older callers see 0 here.
            "skipped_species_drifted": len(drifted_ids),
            "species": species,
        })

    def _load_prediction_rows(db, pred_ids):
        """Load ``pred_ids`` in the shape ``ambiguous_prediction_ids`` wants.

        Selected by id rather than through ``get_predictions(photo_ids=...)``
        so a submitted row is judged on its own merits: the photo-scoped query
        also returns siblings the caller never submitted, and filters to the
        latest ``labels_fingerprint`` — which would silently drop a superseded
        row from the ambiguity check and leave the caller unable to tell "not
        ambiguous" from "not current". Staleness is judged explicitly instead,
        by ``_superseded_prediction_ids``, and reported under its own name.
        Workspace scoping is already settled by ``_parse_prediction_ids``,
        which runs first.

        Chunked for the same reason every other id query here is — a legal
        payload runs past the 999-variable limit older SQLite builds enforce.
        """
        if not pred_ids:
            return []
        ws = db._ws_id()
        rows = []
        for chunk in chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(db.conn.execute(
                f"""SELECT pr.id, pr.species, pr.category, pr.detection_id,
                           pr.source_taxon_id, pr.scientific_name, pr.labels_fingerprint,
                           pr.classifier_model AS model, d.photo_id,
                           pr_rev.group_id AS group_id,
                           pr_rev.individual AS individual
                    FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id
                     AND pr_rev.workspace_id = ?
                    WHERE pr.id IN ({placeholders})""",
                (ws, *chunk),
            ).fetchall())
        return rows

    # The one definition of "already decided" for every prediction-decision
    # endpoint (batch and single-row alike). ``pending`` rows are the normal
    # case; ``alternative`` is not a decision, so this list deliberately omits
    # it — but that only means the rows survive *this* filter. ``batch-reject``
    # then acts on them (a reject sweeps the runners-up down with the winner);
    # ``batch-accept`` still refuses every row on a ``(detection, model)`` that
    # carries one, via ``ambiguous_prediction_ids`` below, because promoting a
    # runner-up picks a winner the user was never shown. Skipped there is
    # reported as ``skipped_ambiguous``, never ``already_decided``: the two
    # counts name different problems with different next steps.
    #
    # ``reviewed`` is included because it *is* a decision: the user pressed
    # "Reviewed" in Review to say "I looked at this and chose not to act". A
    # later accept or reject flip would overwrite that decision the same way a
    # stale accept overwrites a rejected row's sibling scope, and the history
    # entry the flip records reports a "previous" status of ``pending`` — a
    # fiction. Batch endpoints skip the row and report it as
    # ``already_decided``; single-row endpoints refuse with 409, mirroring
    # ``api_mark_prediction_reviewed``'s own precondition.
    # Owned by ``Database`` so ``db.py``'s grouped-accept expansion and this
    # module read the same tuple rather than two copies of it; Browse's panel
    # mirrors the same three values in ``PREDICTION_DECIDED_STATUSES``.
    _DECIDED_PREDICTION_STATUSES = Database.DECIDED_PREDICTION_STATUSES

    def _decided_prediction_ids(db, pred_ids):
        """Which of ``pred_ids`` already have a decision recorded.

        Both batch endpoints need the same precondition, so neither gets to
        pick its own status list: the panel that produced this payload listed
        the rows as pending, but a decision may have landed since — from a
        double-clicked button, a second tab, or Review. Acting on a row whose
        decision is already made writes a history item whose recorded
        "previous" status is a fiction, and the two directions fail in
        mirrored ways: a stale reject leaves an accepted row's species keyword
        attached to a row now marked ``rejected``, and a stale accept tags a
        photo with the loser the user just rejected by accepting its sibling.
        The statuses live here, not at the call sites, so the pair cannot
        drift apart again on what "still actionable" means.

        Chunked: a legal payload runs well past the 999-variable limit older
        SQLite builds enforce (see ``_SQL_PARAM_CHUNK``).
        """
        ws = db._ws_id()
        statuses = _DECIDED_PREDICTION_STATUSES
        if not pred_ids:
            return set()
        status_ph = ",".join("?" for _ in statuses)
        found = set()
        for chunk in chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["prediction_id"] for row in db.conn.execute(
                    f"""SELECT prediction_id FROM prediction_review
                        WHERE workspace_id = ? AND status IN ({status_ph})
                          AND prediction_id IN ({placeholders})""",
                    (ws, *statuses, *chunk),
                )
            )
        return found

    def _prediction_status(db, pred_id):
        """Current review status of one prediction in the active workspace.

        Single-row decision endpoints (accept, reject, mark-reviewed,
        replace-keywords, accept-subject) use this inside
        ``prediction_decisions.begin_prediction_decision`` to enforce the same
        "still actionable" precondition the batch endpoints already enforce
        via ``_decided_prediction_ids``. Same reason the batch helper exists:
        the rule for which statuses are terminal lives in one place, so the
        single-row and batch flavors cannot drift on what "already decided"
        means, and neither flavor can be tightened on one side and forgotten
        on the other.

        Returns ``"pending"`` when no ``prediction_review`` row exists yet,
        the stored status string when one does, and ``None`` when the
        prediction id is unknown. Callers combine that with
        ``_DECIDED_PREDICTION_STATUSES`` to decide whether to 409.
        """
        row = db.conn.execute(
            """SELECT COALESCE(pr_rev.status, 'pending') AS status
               FROM predictions pr
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id
                AND pr_rev.workspace_id = ?
               WHERE pr.id = ?""",
            (db._ws_id(), pred_id),
        ).fetchone()
        return row["status"] if row else None

    def _superseded_prediction_ids(db, pred_ids):
        """Which of ``pred_ids`` belong to a label set the catalog moved past.

        The third precondition both batch endpoints share, alongside
        ``_decided_prediction_ids`` and ``ambiguous_prediction_ids``, and the
        same *shape* as those two: a payload that was truthful when the panel
        rendered it and is not truthful any more. Re-classify a detection
        against a new label set between render and click and the old row stays
        ``pending`` — nothing rewrites it — while ``get_predictions`` (and so
        every panel, Review grid and summary) has already moved to the newest
        ``labels_fingerprint`` for that ``(detection, classifier_model)``.
        Accepting the old row would tag the photo with a species from a label
        set the catalog no longer shows, and mark a row accepted that no
        surface displays; rejecting it would report a dismissal while the
        current row stays pending in the panel the user is looking at.

        A peer helper rather than a branch inside ``ambiguous_prediction_ids``
        because the two verdicts are not the same fact and do not lead the user
        to the same place: an ambiguous row needs a decision in Review, a
        superseded row needs nothing at all — the panel refresh simply shows
        the current row in its place. Folding it in would put superseded rows
        under a ``skipped_ambiguous`` count that names a conflict the user
        would go looking for and never find, which is the sort of quiet
        mis-description ``CORE_PHILOSOPHY.md`` rules out. What *is* shared is
        that the rule has one implementation for both endpoints, so accept and
        reject cannot drift on what "current" means.

        The latest-fingerprint expression is the one ``get_predictions`` and
        ``get_top_prediction_for_photo`` both use: newest ``created_at``, ties
        broken by ``id``.

        Chunked for the same reason every other id query here is (see
        ``_SQL_PARAM_CHUNK``).
        """
        if not pred_ids:
            return set()
        found = set()
        for chunk in chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["id"] for row in db.conn.execute(
                    f"""SELECT pr.id FROM predictions pr
                        WHERE pr.id IN ({placeholders})
                          AND pr.labels_fingerprint != (
                              SELECT pr2.labels_fingerprint FROM predictions pr2
                              WHERE pr2.detection_id = pr.detection_id
                                AND pr2.classifier_model = pr.classifier_model
                              ORDER BY pr2.created_at DESC, pr2.id DESC
                              LIMIT 1)""",
                    chunk,
                )
            )
        return found

    def _parse_observed_statuses(body):
        """Validate ``/api/predictions/group/apply``'s render-time baseline.

        The burst modal sends ``observed``: the status it displayed for each
        group member when it loaded (``{prediction_id: status}``). Returns
        ``(observed, None)`` or ``(None, error_response)``.

        Absent or empty means "no baseline", and the route then applies
        unconditionally — the server can only refuse what the client claims to
        have seen. That is not a hole a caller can pick its way through so
        much as the honest limit of the check; the one caller
        (``review.html``'s ``grmApply``) always sends it, and
        ``test_group_apply_client_sends_the_observed_baseline`` fails if that
        stops being true.
        """
        raw = body.get("observed")
        if raw is None:
            return {}, None
        if not isinstance(raw, dict):
            return None, json_error(
                "observed must be an object mapping prediction id to status"
            )
        observed = {}
        for key, value in raw.items():
            try:
                pred_id = int(key)
            except (TypeError, ValueError):
                return None, json_error(
                    "observed keys must be prediction ids"
                )
            if not isinstance(value, str):
                return None, json_error(
                    "observed values must be status strings"
                )
            observed[pred_id] = value
        return observed, None

    def _group_pick_prediction_ids(db, photo_ids, observed):
        """The prediction rows a group-apply pick accepts, keyed by photo.

        The modal's ``observed`` baseline names the burst member it showed for
        each photo, so a pick accepts exactly those rows. A photo with no
        observed row (an older client sends no baseline) falls back to its
        burst member rows, or failing those its rows that are not
        ``alternative`` runners-up. Other predictions on the photo are left
        alone either way.
        """
        by_photo = {pid: [] for pid in photo_ids}
        if not by_photo:
            return by_photo
        if observed:
            for chunk in chunked(sorted(observed)):
                placeholders = ",".join("?" for _ in chunk)
                for row in db.conn.execute(
                    f"""SELECT pr.id, d.photo_id FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        WHERE pr.id IN ({placeholders})
                        ORDER BY pr.id""",
                    chunk,
                ):
                    if row["photo_id"] in by_photo:
                        by_photo[row["photo_id"]].append(row["id"])
        missing = [pid for pid, ids in by_photo.items() if not ids]
        if missing:
            ws = db._ws_id()
            for chunk in chunked(missing):
                placeholders = ",".join("?" for _ in chunk)
                grouped = {}
                for row in db.conn.execute(
                    f"""SELECT pr.id, d.photo_id,
                               pr_rev.group_id IS NOT NULL AS in_group
                        FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        LEFT JOIN prediction_review pr_rev
                          ON pr_rev.prediction_id = pr.id
                         AND pr_rev.workspace_id = ?
                        WHERE d.photo_id IN ({placeholders})
                          AND COALESCE(pr_rev.status, 'pending') != 'alternative'
                        ORDER BY pr.id""",
                    [ws, *chunk],
                ):
                    by_photo[row["photo_id"]].append(row["id"])
                    if row["in_group"]:
                        grouped.setdefault(row["photo_id"], []).append(row["id"])
                # Prefer the burst member rows: a runner-up an earlier reject
                # turned ``rejected`` is no longer ``alternative``, but it is
                # still not the member the burst modal showed.
                by_photo.update(grouped)
        return by_photo

    def _snapshot_pick_prior_statuses(db, pick_pred_ids):
        """Prior review status of every row a group pick can rewrite, by photo.

        Returns ``{photo_id: {pred_id: status}}`` covering each pick row and
        every sibling in its (detection, classifier model, label set) scope
        that has an explicit ``prediction_review`` entry in the active
        workspace — exactly the rows ``_accept_group_pick_rows`` may write.
        A row with no entry is omitted: the generic reset in
        ``Database._undo_prediction_accept_statuses`` lands it at
        ``alternative`` / ``pending`` on undo, which matches "no row".

        Two re-open cases need this. A pick can promote a member the user
        previously rejected, and a pick can settle a sibling the user had
        already accepted (an accepted alternative, whose grouped primary was
        rejected). The generic scope reset would restore neither decision.
        """
        pick_to_photo = {pred_id: pid
                         for pid, ids in pick_pred_ids.items()
                         for pred_id in ids}
        if not pick_to_photo:
            return {}
        ws = db._ws_id()
        out = {}
        for chunk in chunked(sorted(pick_to_photo)):
            placeholders = ",".join("?" for _ in chunk)
            for row in db.conn.execute(
                f"""SELECT pick.id AS pick_id, pr.id AS prediction_id,
                           pr_rev.status AS status
                    FROM predictions pick
                    JOIN predictions pr
                      ON pr.detection_id = pick.detection_id
                     AND pr.classifier_model = pick.classifier_model
                     AND pr.labels_fingerprint = pick.labels_fingerprint
                    JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id
                     AND pr_rev.workspace_id = ?
                    WHERE pick.id IN ({placeholders})""",
                (ws, *chunk),
            ):
                photo_id = pick_to_photo[row["pick_id"]]
                out.setdefault(photo_id, {})[row["prediction_id"]] = row["status"]
        return out

    def _accept_group_pick_rows(db, pick_pred_ids):
        """Accept each pick's rows and settle their siblings, in-transaction.

        Mirrors a single Accept: within each accepted row's (detection,
        classifier model, label set) scope, ``pending`` and ``alternative``
        siblings become ``rejected``. Unlike a single Accept, an ``accepted``
        sibling is rejected too: the burst modal re-opens decided bursts, so
        a pick can promote a grouped primary whose alternative the user had
        accepted earlier, and leaving that alternative accepted would leave
        two accepted rows in one scope. ``_snapshot_pick_prior_statuses``
        records the sibling's prior ``accepted`` so undo restores it, and
        ``Database._redo_prediction_accept_statuses`` re-rejects it on redo.
        """
        ws = db._ws_id()
        accepted_by_scope = {}
        for ids in pick_pred_ids.values():
            for pred_id in ids:
                db.update_prediction_status(pred_id, "accepted", _commit=False)
                scope = db.conn.execute(
                    """SELECT detection_id, classifier_model, labels_fingerprint
                       FROM predictions WHERE id = ?""",
                    (pred_id,),
                ).fetchone()
                accepted_by_scope.setdefault(tuple(scope), set()).add(pred_id)
        for scope, accepted_ids in accepted_by_scope.items():
            placeholders = ",".join("?" for _ in accepted_ids)
            sibling_ids = [row["id"] for row in db.conn.execute(
                f"""SELECT pr.id FROM predictions pr
                    LEFT JOIN prediction_review pr_rev
                      ON pr_rev.prediction_id = pr.id
                     AND pr_rev.workspace_id = ?
                    WHERE pr.detection_id = ?
                      AND pr.classifier_model = ?
                      AND pr.labels_fingerprint = ?
                      AND pr.id NOT IN ({placeholders})
                      AND COALESCE(pr_rev.status, 'pending')
                          IN ('pending', 'alternative', 'accepted')""",
                (ws, *scope, *sorted(accepted_ids)),
            )]
            for sid in sibling_ids:
                db.update_prediction_status(sid, "rejected", _commit=False)

    def _stale_group_apply_photos(db, observed):
        """Photos whose group member was decided or regenerated since the modal rendered.

        Group apply is the one decision route where "already decided" is *not*
        the right precondition. The single-row and batch endpoints can use it
        because their buttons only exist on pending rows, so a decided row can
        only have been decided by someone else. The burst modal is different:
        it opens on any card carrying a ``group_id`` — including one whose
        members this same user accepted a minute ago — and
        ``loadGroupData`` re-derives picks/rejects from quality scores rather
        than from the stored statuses. Refusing every decided row would block
        a legitimate flow (re-open the burst, change the split, apply) *and*
        would describe the user's own prior decision as somebody else's.

        So the precondition is compare-and-swap against what the modal
        actually displayed: skip a photo only when the picture the modal saw
        has moved. Two shapes of "moved" invalidate a photo, in the same
        pass so the check cannot narrow to one and miss the other:

        1. **Status drift on an observed row.** An observed row's stored
           status is no longer the one the client displayed — a decision that
           landed from Browse or a second tab after the modal opened. A
           deliberate re-decision passes (observed ``accepted`` still matches
           current ``accepted``); a foreign decision does not.
        2. **Superseded label set.** An observed row is no longer the latest
           ``labels_fingerprint`` for its ``(detection, classifier_model)`` —
           classification reran between render and click and inserted new
           prediction rows for the same detections. The old row still exists
           with an unchanged status, so shape 1 alone would clear the check.
           But the write below (``update_predictions_status_by_photo``) does
           not respect the observed set: it rewrites every prediction on the
           photo, including the newly inserted rows the modal never saw, and
           applies the modal's stale species to them.

        Photo-level because the write is: ``update_predictions_status_by_photo``
        restates every prediction of the photo, so one stale member
        invalidates the whole photo's write, not just its own row.

        The latest-fingerprint expression is the one
        ``_superseded_prediction_ids``, ``get_predictions`` and
        ``get_top_prediction_for_photo`` all use: newest ``created_at``, ties
        broken by ``id``. Keeping one definition of "current" everywhere is
        the property the rest of this PR's precondition family relies on.
        """
        if not observed:
            return set()
        ws = db._ws_id()
        stale = set()
        for chunk in chunked(list(observed)):
            placeholders = ",".join("?" for _ in chunk)
            for row in db.conn.execute(
                f"""SELECT pr.id AS prediction_id, d.photo_id AS photo_id,
                           COALESCE(pr_rev.status, 'pending') AS status,
                           (pr.labels_fingerprint != (
                               SELECT pr2.labels_fingerprint FROM predictions pr2
                               WHERE pr2.detection_id = pr.detection_id
                                 AND pr2.classifier_model = pr.classifier_model
                               ORDER BY pr2.created_at DESC, pr2.id DESC
                               LIMIT 1
                           )) AS is_superseded
                      FROM predictions pr
                      JOIN detections d ON d.id = pr.detection_id
                      LEFT JOIN prediction_review pr_rev
                        ON pr_rev.prediction_id = pr.id
                       AND pr_rev.workspace_id = ?
                     WHERE pr.id IN ({placeholders})""",
                (ws, *chunk),
            ):
                if (row["status"] != observed[row["prediction_id"]]
                        or row["is_superseded"]):
                    stale.add(row["photo_id"])
        return stale

    def _species_drifted_prediction_ids(db, rows, expected_species):
        """Which of ``rows`` no longer resolve to the species Browse rendered.

        Both accept panels group by species and label the button with the
        species they will apply — "Accept on 38 Bald Eagle". If the row's
        grouping changes between render and click, ``accept_prediction``
        will now apply a different species than the button named. Two known
        shapes:

        * ``/api/predictions/group/apply`` in another tab ungroups a burst
          member, so the row's ``individual`` votes are cleared and
          ``SpeciesResolver.consensus`` falls back to the raw per-frame
          label — a Robin-consensus row for a frame whose own species column
          reads Sparrow now accepts as Sparrow, not the Robin the button
          named.
        * Re-running consensus after a per-vote edit shifts the winner (Robin
          4 → Robin 2, Sparrow 4).

        Both let the endpoint tag the photo with a species the caller was
        never shown as the target of this click, and the shared "all resolve
        to one species" precondition below doesn't catch it when *every* row
        drifts to the same new species.

        No-ops when ``expected_species`` is falsy — a caller with no species
        in hand (Review's single-photo routes, older tests) is out of scope
        for this check. Compare resolved identities so common/scientific aliases
        do not register as drift. Unresolved labels retain text matching.

        Returns the drifted-row subset of ``rows`` ids.
        """
        rows = list(rows)
        if not rows or not expected_species:
            return set()
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        expected_key = resolver.display(expected_species).key
        drifted = set()
        for row in rows:
            current = resolver.consensus(row)
            if current.key != expected_key and keyword_match_key(current.display_name) != keyword_match_key(expected_species):
                drifted.add(row["id"])
        return drifted

    def _parse_prediction_ids(db, body):
        """Validate a batch payload's ``prediction_ids``.

        Returns ``(prediction_ids, None)`` or ``(None, error_response)``.
        Every id must exist and belong to a photo in the active workspace, so
        a batch can never reach across a workspace boundary.

        Size is bounded in the one unit the producer bounds: photos. These
        payloads come from ``/api/selection/prediction-suggestions``, which
        accepts at most ``MAX_SELECTION_PHOTOS`` photos and then emits every
        matching prediction row for them — a count it does not (and should
        not) cap, since a photo legitimately carries one row per detection per
        classifier model. Counting *ids* here therefore cannot be done without
        inventing a rows-per-photo guess, and three rounds of review found the
        guess wrong each time (1,000, then 25,000, then 200,000). Counting
        distinct photos instead makes the invariant hold by construction: any
        payload the suggestions endpoint can legally emit spans at most the
        photo selection it was given, so this endpoint accepts it — no margin
        to re-tune.
        """
        raw_ids = body.get("prediction_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return None, json_error("prediction_ids required")
        pred_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None, json_error("prediction_ids must be integers")
            if raw not in seen:
                pred_ids.append(raw)
                seen.add(raw)

        # Chunked because a legal payload runs to many thousands of ids, and a
        # single IN clause that wide exceeds the 999-variable limit older
        # SQLite builds enforce. ``_SQL_PARAM_CHUNK`` is the module-wide
        # convention for exactly this.
        photo_by_pred = {}
        for chunk in chunked(pred_ids):
            placeholders = ",".join("?" for _ in chunk)
            photo_by_pred.update({
                row["id"]: row["photo_id"] for row in db.conn.execute(
                    f"""SELECT pr.id, d.photo_id
                        FROM predictions pr
                        JOIN detections d ON d.id = pr.detection_id
                        WHERE pr.id IN ({placeholders})""",
                    chunk,
                ).fetchall()
            })
            # Bail as soon as the payload outgrows a legal selection rather
            # than resolving the rest of a runaway request.
            if len(set(photo_by_pred.values())) > MAX_SELECTION_PHOTOS:
                return None, json_error("too many photos in selection", 400)
        for pid in pred_ids:
            if pid not in photo_by_pred:
                return None, json_error(f"Prediction {pid} not found", 404)
        # One workspace query per distinct photo, not per prediction id:
        # several rows on one photo must not cost several round trips each.
        for photo_id in dict.fromkeys(photo_by_pred.values()):
            if not db._photo_in_workspace(photo_id):
                return None, json_error(
                    f"Photo {photo_id} does not belong to the "
                    "active workspace", 403,
                )
        return pred_ids, None

    @blueprint.route("/api/predictions/batch-reject", methods=["POST"])
    def api_batch_reject_predictions():
        """Reject many predictions as a single undoable action.

        A photo with several detections carries one prediction row per
        detection, so Browse groups them into one species row. Dismissing that
        row must be one Cmd-Z, not one per detection.

        Contract, and the transaction that backs it, are ``batch-accept``'s:
        the preconditions and the writes share one ``BEGIN IMMEDIATE``
        transaction, so an Accept and a Reject fired before the panel reloads
        are serialized instead of interleaved. Rows already decided, or from a
        superseded label set, are skipped and counted back.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        pred_ids, err = _parse_prediction_ids(db, body)
        if err is not None:
            return err

        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            return _batch_reject_under_lock(db, pred_ids)
        except Exception:
            db.conn.rollback()
            raise

    def _batch_reject_under_lock(db, pred_ids):
        """The checks and writes of ``batch-reject``, inside its transaction."""
        # The same filter ``batch-accept`` applies, through the same helper —
        # not a parallel copy that can be tightened on one side only. Without
        # it, a stale panel — or an Accept then Reject before the panel
        # reloads — overwrites an ``accepted`` row with ``rejected`` while the
        # species keyword the accept added stays on the photo: keyword state
        # and review state then contradict each other, and nothing in the UI
        # points at the contradiction. Re-rejecting an already ``rejected``
        # row is skipped for the matching reason: it appends a history item
        # whose ``old_value`` of "pending" never happened.
        already_decided = _decided_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in already_decided]

        # Superseded rows are skipped on this side too, through the same
        # helper. A reject writes no keyword, so the damage is smaller than a
        # stale accept's — but it is the same misreport: the user dismisses a
        # species, the row that vanished from every panel is the one marked
        # ``rejected``, and the current prediction the panel *does* show stays
        # pending. One rule, one implementation, both endpoints — the lesson
        # the status precondition already taught here.
        superseded_ids = _superseded_prediction_ids(db, pred_ids)
        pred_ids = [pid for pid in pred_ids if pid not in superseded_ids]

        # Same shape as batch-accept: the workspace half of
        # ``_parse_prediction_ids`` runs outside the lock and a folder detach
        # can slip through in the parse→lock window. A reject writes no
        # keyword, but a workspace-scoped ``prediction_review`` row for a
        # now-foreign photo is the same class of leak the accept side
        # closes, and both endpoints filter identically for the same reason
        # they share ``_decided_prediction_ids``: a rule with two
        # implementations drifts.
        out_of_workspace_ids = (
            prediction_decisions.out_of_workspace_prediction_ids(db, pred_ids)
        )
        pred_ids = [pid for pid in pred_ids if pid not in out_of_workspace_ids]

        ws = db._ws_id()
        items = []
        species = None
        for pid in pred_ids:
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, pr.detection_id,
                          pr.classifier_model AS model,
                          pr.labels_fingerprint, d.photo_id
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   WHERE pr.id = ?""",
                (pid,),
            ).fetchone()
            if pred is None:
                continue
            species = species or pred["species"]
            db.update_prediction_status(pid, "rejected", _commit=False)
            # Sibling alternatives go down with the parent, scoped by
            # fingerprint so a new label set can't rewrite an old one's
            # review state — same rule as the single-prediction reject.
            for row in db.conn.execute(
                """SELECT pr.id FROM predictions pr
                   JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.detection_id = ? AND pr.classifier_model = ?
                     AND pr.labels_fingerprint = ? AND pr.id != ?
                     AND pr_rev.status = 'alternative'""",
                (ws, pred["detection_id"], pred["model"],
                 pred["labels_fingerprint"], pid),
            ).fetchall():
                db.update_prediction_status(row["id"], "rejected", _commit=False)
            items.append({
                "photo_id": pred["photo_id"],
                "old_value": "pending",
                "new_value": "rejected",
            })

        # In the same transaction as the statuses it records, for the reason
        # ``batch-accept`` gives.
        if items:
            photo_count = len({item["photo_id"] for item in items})
            desc = f'Rejected prediction "{species}"'
            if photo_count > 1:
                desc += f" on {photo_count} photos"
            db.record_edit(
                "prediction_reject", desc, "rejected", items,
                is_batch=photo_count > 1, _commit=False,
            )
        db.conn.commit()
        if items:
            db._prune_edit_history()
        return jsonify({
            "ok": True,
            "rejected": len(items),
            # Reported rather than folded into ``rejected``, under the same
            # name batch-accept uses: a caller that resubmits should be able
            # to tell "nothing to do, already decided" from "nothing matched".
            "already_decided": len(already_decided),
            # Same name and meaning as on the accept side: rows a later
            # classification run replaced.
            "skipped_superseded": len(superseded_ids),
            # Same name and meaning as on the accept side: rows whose photo
            # left the workspace between parse and lock. Reported so a
            # panel-refresh caller can distinguish "detached mid-flight"
            # from "already rejected" or "superseded".
            "skipped_out_of_workspace": len(out_of_workspace_ids),
        })

    @blueprint.route("/api/predictions/<int:pred_id>/accept", methods=["POST"])
    def api_accept_prediction(pred_id):
        """Accept a single prediction as one atomic decision.

        Serialized with every other prediction-decision route through
        ``prediction_decisions.begin_prediction_decision`` — batch-accept,
        batch-reject, single reject, replace-keywords, accept-subject,
        mark-reviewed. Without the lock, a double-clicked Accept, or an Accept
        fired while a batch-reject for the same row is in flight, can both
        pass their status precondition against the same pre-write state and
        both commit. The reject wins the write; the accept's keyword stays on
        the photo; keyword state and review state then contradict each other
        and undo restores a state that never existed. Holding SQLite's writer
        lock across the read *and* the write makes the check-then-write
        indivisible, exactly as the batch endpoints already do.
        """
        db = get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            current_status = _prediction_status(db, pred_id)
            # Missing row falls through to ``accept_prediction`` returning None
            # — the endpoint's historical contract for unknown ids is a 200
            # no-op, not a 404.
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            result = db.accept_prediction(pred_id, _commit=False)
            if result and result["affected"]:
                # ``changed_tag=False`` means the photo already carried an
                # equivalent species (hierarchical or root) so the accept only
                # flipped ``prediction_review.status``. Encode that as a JSON
                # ``no_tag`` payload so undo/redo can reverse the status change
                # without untagging (or re-tagging) a keyword the user
                # deliberately kept. Regular accepts still use the compact
                # ``str(prediction_id)`` form so existing edit-history rows
                # keep parsing unchanged.
                def _make_item(a):
                    if a.get('changed_tag', True):
                        old_value = str(a['prediction_id'])
                    else:
                        old_value = json.dumps({
                            'prediction_id': a['prediction_id'],
                            'no_tag': True,
                        })
                    return {
                        'photo_id': a['photo_id'],
                        'old_value': old_value,
                        'new_value': str(result['keyword_id']),
                    }
                items = [_make_item(a) for a in result['affected']]
                is_batch = len(result['affected']) > 1
                desc = f'Accepted prediction: added "{result["species"]}"'
                if is_batch:
                    desc += f' to {len(result["affected"])} photos'
                db.record_edit(
                    'prediction_accept', desc, str(result['keyword_id']),
                    items, is_batch=is_batch, _commit=False,
                )
            db.conn.commit()
            # Name every row this call decided, not just the one in the URL.
            # A grouped accept expands through the burst, so a caller looping
            # over a selection can have its next row already decided *by this
            # response* — and would otherwise meet the 409 above and report a
            # decision that did land as "not applied". Reporting the ids is
            # the only version of that answer the caller can trust: it is what
            # this transaction wrote, not an inference from a later status
            # read that cannot say who wrote it. Matches the shape
            # ``accept-subject`` already returns.
            return jsonify({
                "ok": True,
                "prediction_ids": (
                    result["accepted_prediction_ids"] if result else []
                ),
                "photo_ids": result["photo_ids"] if result else [],
            })
        except Exception:
            db.conn.rollback()
            raise

    @blueprint.route("/api/predictions/<int:pred_id>/accept-subject", methods=["POST"])
    def api_accept_subject_species(pred_id):
        """Accept an additional-subject species from Compare, atomically.

        Under the same lock as every other prediction-decision route (see
        ``api_accept_prediction``). The precondition check is on the entry
        row's status: if the user marked it reviewed, or a race with a batch
        endpoint already accepted/rejected it, this endpoint must refuse
        rather than overwrite that decision.
        """
        db = get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            current_status = _prediction_status(db, pred_id)
            if current_status is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if current_status in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f"prediction already {current_status}; cannot accept",
                    409,
                )
            result = db.accept_subject_species(pred_id, _commit=False)
            if result is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if result["affected"]:
                # Accept-subject can accept agreeing predictions from
                # multiple classifier models on one detection. Undo/redo
                # must reset every sibling scope, so always encode the full
                # ``prediction_ids`` list as JSON — the compact
                # comma-separated fallback cannot be parsed by
                # ``_edit_prediction_ids`` and would drop every sibling
                # status flip. ``no_tag`` is set only when every underlying
                # accept was a no-op (photo already carried the target via
                # an equivalent hierarchical or root row); in mixed batches
                # one accept actually tagged the species, so undo must
                # untag exactly once.
                _all_no_tag = all(
                    not a.get("changed_tag", True) for a in result["affected"]
                )
                payload = {
                    "prediction_ids": [
                        int(pid) for pid in result["prediction_ids"]
                    ],
                }
                if _all_no_tag:
                    payload["no_tag"] = True
                old_value = json.dumps(payload)
                db.record_edit(
                    "prediction_accept",
                    f'Accepted additional subject species: added "{result["species"]}"',
                    str(result["keyword_id"]),
                    [{
                        "photo_id": result["photo_id"],
                        "old_value": old_value,
                        "new_value": str(result["keyword_id"]),
                    }],
                    _commit=False,
                )
            db.conn.commit()
            return jsonify({
                "ok": True,
                "species": result["species"],
                "prediction_ids": result["prediction_ids"],
                "photo_ids": [result["photo_id"]],
            })
        except Exception:
            db.conn.rollback()
            raise

    @blueprint.route("/api/predictions/<int:pred_id>/reject", methods=["POST"])
    def api_reject_prediction(pred_id):
        """Reject a single prediction as one atomic decision.

        Serialized with every other prediction-decision route through
        ``prediction_decisions.begin_prediction_decision`` — see
        ``api_accept_prediction`` for the full argument. Codex's fresh
        evidence beyond the batch-atomicity fix: a Browse batch accept
        overlapping a Review-side single reject on the same row would let the
        reject read while the batch held the writer lock and then overwrite
        the newly accepted status *after* the batch committed, leaving the
        species keyword attached to a row now marked rejected. Same failure
        mode as the inverse (single accept vs batch reject). The single-row
        routes now take the same lock and honour the same terminal-status
        precondition as their batch siblings.
        """
        db = get_db()
        lock_err = prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )
        if lock_err is not None:
            return lock_err
        try:
            if prediction_decisions.out_of_workspace_prediction_ids(db, [pred_id]):
                db.conn.rollback()
                return json_error("Prediction does not belong to the active workspace", 404)
            # Review state lives in prediction_review now; predictions.model
            # is renamed to classifier_model. Sibling-alternative rejection
            # goes through the workspace-scoped review table.
            ws = db._ws_id()
            pred = db.conn.execute(
                """SELECT pr.id, pr.species, pr.detection_id,
                          pr.classifier_model AS model,
                          pr.labels_fingerprint, d.photo_id,
                          pr_rev.group_id,
                          COALESCE(pr_rev.status, 'pending') AS status
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.id = ?""",
                (ws, pred_id),
            ).fetchone()
            # prediction_review has an FK on prediction_id; writing review
            # state for a missing pred would raise an IntegrityError and
            # return 500 where the legacy endpoint returned a harmless
            # no-op. Gate the write on existence so stale IDs stay a clean
            # 404.
            if pred is None:
                db.conn.rollback()
                return json_error("prediction not found", 404)
            if pred["status"] in _DECIDED_PREDICTION_STATUSES:
                db.conn.rollback()
                return json_error(
                    f'prediction already {pred["status"]}; cannot reject',
                    409,
                )
            # A burst card's "Not X" answers for the whole burst, the way its
            # "Tag N photos" accept does (``accept_prediction`` expands across
            # the group). Rejecting only the lead left the other members
            # pending and hidden behind a card that read "Rejected", where a
            # later Accept All would tag them X. Expansion follows the accept
            # rule: it only discovers members that are still undecided.
            targets = [(pred["photo_id"], pred_id)]
            if pred["group_id"]:
                for gp in db.conn.execute(
                    """SELECT pr.id, d.photo_id,
                              COALESCE(pr_rev.status, 'pending') AS status
                       FROM predictions pr
                       JOIN prediction_review pr_rev
                         ON pr_rev.prediction_id = pr.id
                        AND pr_rev.workspace_id = ?
                       JOIN detections d ON d.id = pr.detection_id
                       JOIN photos ph ON ph.id = d.photo_id
                       JOIN workspace_folders wf
                         ON wf.folder_id = ph.folder_id AND wf.workspace_id = ?
                       WHERE pr_rev.group_id = ? AND pr.classifier_model = ?
                         AND pr.id != ?
                       ORDER BY pr.id""",
                    (ws, ws, pred["group_id"], pred["model"], pred_id),
                ):
                    if gp["status"] not in _DECIDED_PREDICTION_STATUSES:
                        targets.append((gp["photo_id"], gp["id"]))
            rejected_ids = []
            for _target_photo, target_id in targets:
                rejected_ids.append(target_id)
                _reject_prediction_row(db, ws, target_id)
            target_photos = list(dict.fromkeys(t[0] for t in targets))
            desc = f'Rejected prediction "{pred["species"]}"'
            if len(target_photos) > 1:
                desc += f" on {len(target_photos)} photos"
            db.record_edit(
                'prediction_reject',
                desc,
                'rejected',
                [{
                    'photo_id': target_photo,
                    'old_value': 'pending',
                    'new_value': 'rejected',
                } for target_photo in target_photos],
                is_batch=len(target_photos) > 1,
                _commit=False,
            )
            db.conn.commit()
            return jsonify({"ok": True, "rejected_prediction_ids": rejected_ids})
        except Exception:
            db.conn.rollback()
            raise

    def _reject_prediction_row(db, ws, pred_id):
        """Reject one prediction and its ``alternative`` siblings, in-transaction."""
        scope = db.conn.execute(
            """SELECT detection_id, classifier_model, labels_fingerprint
               FROM predictions WHERE id = ?""",
            (pred_id,),
        ).fetchone()
        db.update_prediction_status(pred_id, "rejected", _commit=False)
        # Also reject sibling alternative predictions for the same
        # (detection, classifier_model, labels_fingerprint) in this
        # workspace. Fingerprint scoping matters: without it, rejecting a
        # prediction from a new label set would rewrite review state for
        # prior fingerprints' alternatives on the same detection.
        sibling_ids = [row["id"] for row in db.conn.execute(
            """SELECT pr.id
               FROM predictions pr
               JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id
                AND pr_rev.workspace_id = ?
               WHERE pr.detection_id = ?
                 AND pr.classifier_model = ?
                 AND pr.labels_fingerprint = ?
                 AND pr.id != ?
                 AND pr_rev.status = 'alternative'""",
            (ws, scope["detection_id"], scope["classifier_model"],
             scope["labels_fingerprint"], pred_id),
        ).fetchall()]
        for sid in sibling_ids:
            db.update_prediction_status(sid, "rejected", _commit=False)

    @blueprint.route("/api/predictions/group/<group_id>")
    def api_prediction_group(group_id):
        """Get all predictions and photo data for a burst group."""
        db = get_db()
        preds = db.get_group_predictions(group_id)
        rows = [dict(p) for p in preds]
        attach_nested_edit_recipes(db, rows)
        return jsonify(rows)

    @blueprint.route("/api/predictions/group/apply", methods=["POST"])
    def api_prediction_group_apply():
        """Apply pick/reject decisions and species to a burst group."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        picks = body.get("picks", [])  # list of photo_ids
        rejects = body.get("rejects", [])  # list of photo_ids
        removed = body.get("removed", [])  # list of prediction_ids to ungroup
        species = body.get("species", "")
        observed, observed_err = _parse_observed_statuses(body)
        if observed_err is not None:
            return observed_err

        # Pre-validate all photo IDs against workspace before any mutations
        for pid in picks + rejects:
            if not db._photo_in_workspace(pid):
                return json_error(f"Photo {pid} is not in the active workspace", 403)

        # Everything below — the stale filter, the keyword/flag writes, and
        # the prediction status writes — runs under the single decision lock
        # in one transaction. An earlier version filtered stale photos and
        # committed the flag/keyword writes *before* taking the lock and then
        # re-checked staleness inside it; a decision that landed in that
        # window would leave the photo flagged and keyworded while the
        # in-lock recheck refused to update its prediction status, so the
        # keyword-on-a-rejected-row split this precondition exists to
        # prevent came back as a narrower race. The fix is to hold the write
        # lock across the read that governs the writes.
        def _apply_group_decisions():
            # Stale filter under the lock: a decision that committed since
            # the modal rendered wins over this apply, so those photos are
            # dropped before any of *this* apply's writes land. Doing the
            # check inside ``BEGIN IMMEDIATE`` closes the check-then-write
            # window — nothing can decide between this read and the writes
            # below, so a photo we skip here cannot silently be flagged or
            # keyworded on the other side of the transaction.
            stale_photos = _stale_group_apply_photos(db, observed)
            actionable_picks = [pid for pid in picks if pid not in stale_photos]
            actionable_rejects = [
                pid for pid in rejects if pid not in stale_photos
            ]

            # Capture old flag values before mutation
            all_flag_pids = actionable_picks + actionable_rejects
            old_flags = {}
            for pid in all_flag_pids:
                old = db.get_photo(pid)
                if old:
                    old_flags[pid] = old["flag"] or "none"

            local_species = species
            # Flag picks and add species keyword — every write uses
            # ``_commit=False`` because the enclosing ``BEGIN IMMEDIATE`` owns
            # the transaction and an intermediate commit would release the
            # writer lock mid-decision.
            try:
                if local_species:
                    kid = db.add_keyword(
                        local_species, is_species=True, _commit=False,
                    )
                    # Queue/record the stored spelling (see api_add_keyword).
                    stored = db.conn.execute(
                        "SELECT name FROM keywords WHERE id = ?", (kid,)
                    ).fetchone()
                    if stored and stored["name"]:
                        local_species = stored["name"]
                    already_has_species = db.get_photos_with_equivalent_species(
                        actionable_picks, kid
                    )
                    added_picks = []
                    for pid in actionable_picks:
                        db.update_photo_flag(pid, "flagged", _commit=False)
                        if pid in already_has_species:
                            continue
                        db.tag_photo(pid, kid, source="manual", _commit=False)
                        db.queue_change(
                            pid, "keyword_add", local_species, _commit=False,
                        )
                        added_picks.append(pid)

                else:
                    # No species — still flag picks
                    kid = None
                    already_has_species = set()
                    added_picks = []
                    for pid in actionable_picks:
                        db.update_photo_flag(pid, "flagged", _commit=False)

                # Reject rejects
                for pid in actionable_rejects:
                    db.update_photo_flag(pid, "rejected", _commit=False)
            except ValueError as e:
                # ``prediction_decisions.under_prediction_decision_lock``'s
                # finally will roll back the still-open transaction; returning
                # here just short-circuits the rest of the writes.
                return json_error(str(e), 403)

            # Record flag history for all picks + rejects
            flag_items = []
            for pid in actionable_picks:
                if pid in old_flags:
                    flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'flagged'})
            for pid in actionable_rejects:
                if pid in old_flags:
                    flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'rejected'})
            if flag_items:
                for item in flag_items:
                    db.queue_flag_change_if_enabled(
                        item["photo_id"], item["new_value"], _commit=False
                    )
                desc = (
                    f'Group prediction: flagged {len(actionable_picks)}, '
                    f'rejected {len(actionable_rejects)}'
                )
                db.record_edit(
                    'flag', desc, 'group_apply', flag_items,
                    is_batch=True, _commit=False,
                )

            # A pick accepts its burst member, the way a single Accept does:
            # the member row becomes ``accepted`` and the open siblings in its
            # (detection, model, label set) scope become ``rejected``. Writing
            # ``accepted`` over every prediction on the photo also accepted
            # the alternative species the modal only listed as runners-up.
            pick_pred_ids = _group_pick_prediction_ids(
                db, actionable_picks, observed,
            )
            # Snapshot the pre-apply status of every row the accept can
            # rewrite (the picks and their scope siblings) so undo restores
            # it. Without this, a re-open of an already-applied burst would
            # undo a previously rejected pick back to ``pending``, and a
            # previously accepted sibling back to ``alternative``; the
            # generic scope reset in
            # ``Database._undo_prediction_accept_statuses`` cannot tell that
            # a row wasn't at the default state before the accept.
            prior_pick_statuses = _snapshot_pick_prior_statuses(
                db, pick_pred_ids,
            )
            _accept_group_pick_rows(db, pick_pred_ids)

            # One history entry covers the keyword and the statuses, so undo
            # restores both together. Before this the statuses were never
            # recorded: undo removed the keyword but left the rows accepted,
            # and a later single Accept of the same photo answered 409.
            accept_items = []
            for pid in actionable_picks:
                ids = pick_pred_ids.get(pid) or []
                tagged = pid in added_picks
                if not ids and not tagged:
                    continue
                if ids:
                    meta = {"prediction_id": ids[0], "prediction_ids": ids}
                    if not tagged:
                        meta["no_tag"] = True
                    photo_priors = prior_pick_statuses.get(pid) or {}
                    if photo_priors:
                        # JSON object keys are strings; keep the encoded form
                        # so ``_undo_prediction_accept_statuses`` sees the
                        # same shape after ``json.loads`` on undo.
                        meta["prior_statuses"] = {
                            str(pred_id): status
                            for pred_id, status in photo_priors.items()
                        }
                    old_value = json.dumps(meta)
                else:
                    old_value = ''
                accept_items.append({
                    'photo_id': pid,
                    'old_value': old_value,
                    'new_value': str(kid) if kid is not None else '',
                })
            if accept_items:
                has_rows = any(pick_pred_ids.get(pid) for pid in actionable_picks)
                n = len(accept_items)
                if kid is not None:
                    desc = f'Added "{local_species}" to {n} photos (group prediction)'
                else:
                    desc = f'Accepted group prediction on {n} photos'
                db.record_edit(
                    'prediction_accept' if has_rows else 'keyword_add',
                    desc,
                    str(kid) if kid is not None else '',
                    accept_items,
                    is_batch=n > 1,
                    _commit=False,
                )

            for pid in actionable_rejects:
                db.update_predictions_status_by_photo(
                    pid, 'rejected', _commit=False,
                )

            # Remove predictions from group. Ungrouping is not a decision — it
            # changes which rows the burst modal shows together, not what any
            # of them means — so it is not gated on the baseline.
            for pred_id in removed:
                db.ungroup_prediction(pred_id, _commit=False)
            db.conn.commit()
            # ``record_edit`` skips its prune under ``_commit=False``; run it
            # once the decision is durable so history stays bounded — same
            # shape the batch endpoints use.
            db._prune_edit_history()
            # Counted in photos, which is the unit the modal works in and the
            # unit the toast names. Reported even when zero so the client can
            # tell "nothing was stale" from an older server that never checked.
            return jsonify({"ok": True, "already_decided": len(stale_photos)})

        return prediction_decisions.under_prediction_decision_lock(
            db, _apply_group_decisions, json_error=json_error,
        )

    return blueprint
