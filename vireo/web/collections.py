"""Collections: saved rule sets (smart and static) scoped to a workspace.

``/api/collections`` lists, creates, previews, updates, deletes and duplicates
collections, appends photos to a static collection, and pages through a
collection's photos or ids. ``GET /api/collections`` and
``GET /api/collections/<id>/photos`` are also the stable
``/api/v1/collections`` surface (see ``docs/headless-api.md``); ``create_app``
aliases them by their ``collections.``-qualified endpoint names.
"""

from __future__ import annotations

import json

import config as cfg
from flask import Blueprint, current_app, jsonify, request
from photo_payload import prepare_browse_photo_dicts
from services.visual_scope import (
    collection_rules_state,
    inject_active_visual_model,
    validate_visual_arg,
)
from web.request_args import (
    MAX_FOCUS_PHOTO_IDS,
    focus_candidate_ids,
    reject_visual_collection,
    request_bool_arg,
)


def _collection_accepts_manual_photos(rules):
    """Return True when collection rules are static photo-id membership only."""
    if isinstance(rules, list):
        return all(_collection_accepts_manual_photos(child) for child in rules)

    if not isinstance(rules, dict):
        return False

    if rules.get("field") == "photo_ids":
        return isinstance(rules.get("value", []), list)

    if "rules" in rules and "field" not in rules:
        return (
            rules.get("mode", "all") == "all"
            and isinstance(rules.get("rules"), list)
            and all(
                _collection_accepts_manual_photos(child)
                for child in rules.get("rules")
            )
        )

    return False


def create_collections_blueprint(get_db, json_error, *, max_per_page):
    """Build the collections blueprint.

    ``max_per_page`` is ``create_app``'s page-size cap, shared with the
    ``/api/photos`` listing routes, so a collection page and a Browse page
    clamp ``per_page`` to the same bound.
    """
    blueprint = Blueprint("collections", __name__)

    @blueprint.route("/api/collections")
    def api_collections():
        db = get_db()
        collections = db.get_collections()
        result = []
        for c in collections:
            d = dict(c)
            # A single collection with an unresolvable rule must not 500 the
            # whole list — that blanks every collection dropdown in the UI.
            # Degrade to an unknown count and flag it so the client can show
            # the collection instead of hiding all of them.
            #
            # Only rule-validation failures degrade to count_error: the pickers
            # tell users to fix the rule, so labelling a locked/corrupt DB or a
            # bad generated query as count_error would send them to edit rules
            # that aren't the actual problem. ValueError covers both malformed
            # rules (raised by _build_query_from_rules) and malformed JSON in
            # the rules column (json.JSONDecodeError is a ValueError subclass).
            # Anything else bubbles up as a 500 so real infra failures surface.
            #
            # Visual collections narrow the result set at open time via the
            # filter bar's visual clause; ``count_collection_photos`` only
            # evaluates ``rules``, so a saved visual collection with e.g.
            # ``rules: []`` would return the workspace-wide metadata count
            # here while Browse shows a much smaller set on reopen
            # (Codex review r3620935217). Resolving the visual clause on
            # every listing would run the embedding for every visual
            # collection on every page load — too expensive. Omit the count
            # instead: the sidebar (browse.html) renders an empty count
            # span for null photo_count, and the ✦ visual marker already
            # tells users this collection is visually resolved.
            #
            # Still validate the metadata rules though — skipping the count
            # was also the only validation path that caught malformed JSON
            # or unresolvable rule fields. Without this check, a visual
            # collection with degraded rules would look healthy in the
            # sidebar and only 400 later when Browse routes those bad rules
            # through ``/api/photos/query`` (Codex review r3621304875).
            if c["visual_json"] is not None:
                d["photo_count"] = None
                d["available_photo_count"] = None
                d["offline_photo_count"] = None
                _, degraded = collection_rules_state(db, c["rules"])
                if degraded:
                    d["count_error"] = True
                    current_app.logger.warning(
                        "Visual collection %s (%s) has unresolvable rules; marking degraded",
                        c["id"],
                        c["name"],
                    )
            else:
                try:
                    counts = db.count_collection_photo_availability(c["id"])
                    d["photo_count"] = counts["total"]
                    d["available_photo_count"] = counts["available"]
                    d["offline_photo_count"] = counts["offline"]
                except ValueError:
                    current_app.logger.exception(
                        "Failed to count photos for collection %s (%s)",
                        c["id"],
                        c["name"],
                    )
                    d["photo_count"] = None
                    d["available_photo_count"] = None
                    d["offline_photo_count"] = None
                    d["count_error"] = True
            # Degraded rows must never advertise manual-add support: the
            # add-to-collection modal filters only on can_add_photos, and
            # /api/collections/<id>/add-photos calls set(ids_rule["value"])
            # on the existing rule — which 500s on any malformed photo_ids
            # payload (non-scalar entries, non-list value, etc.). If the
            # rule is bad enough to fail count, it isn't safe to merge into.
            if d.get("count_error"):
                d["can_add_photos"] = False
            elif c["visual_json"] is not None:
                # Visual collections store ``rules: []`` which
                # ``_collection_accepts_manual_photos`` treats as addable,
                # but ``/add-photos`` would only append to ``photo_ids``
                # and leave ``visual_json`` alone — the manually added
                # photos would only surface in the reopened collection
                # if they also matched the hidden visual prompt, so
                # the add is silently ineffective (Codex r3620791304).
                d["can_add_photos"] = False
            else:
                try:
                    d["can_add_photos"] = _collection_accepts_manual_photos(
                        json.loads(c["rules"])
                    )
                except (TypeError, ValueError):
                    d["can_add_photos"] = False
            # ``has_visual`` lets clients that funnel a collection through
            # the legacy rules-only path (``/api/collections/<id>/photos``,
            # ``/photo-ids``, pipeline/Misses ``collection_id`` callers) hide,
            # disable, or reject visual collections. Those paths
            # evaluate ``rules`` only — a visual-only collection would
            # otherwise silently widen the scope to every metadata match.
            # Browse opens the same collection into the filter bar, where
            # the visual clause IS resolved (see collectionsById /
            # loadExpression), so surfacing the flag here lets the two
            # flows behave differently without every caller re-parsing
            # ``visual_json``.
            d["has_visual"] = c["visual_json"] is not None
            result.append(d)
        return jsonify(result)

    @blueprint.route("/api/collections", methods=["POST"])
    def api_create_collection():
        db = get_db()
        body = request.get_json(silent=True) or {}
        import json

        name = body.get("name", "").strip()
        rules = body.get("rules", [])
        if not name:
            return json_error("name required")
        try:
            # Pin ``has_visual_index`` leaves to the active model before
            # counting/persisting so the sidebar count on reopen matches
            # what the save-time preview counter showed — otherwise a
            # library with embeddings from multiple models silently
            # widens the collection to "any embedding exists" (Codex
            # review r3621749904).
            rules = inject_active_visual_model(rules)
            db.count_photos_for_rules(rules)
            # The visual clause is saved alongside rules — a collection
            # saved from an expression with a visual component must
            # reproduce the same result set on reopen, not silently drop
            # to metadata-only.
            visual = validate_visual_arg(body.get("visual"))
        except ValueError as e:
            return json_error(str(e), 400)
        cid = db.add_collection(
            name, json.dumps(rules),
            visual_json=json.dumps(visual) if visual else None,
        )
        return jsonify({"ok": True, "id": cid})

    @blueprint.route("/api/collections/preview", methods=["POST"])
    def api_collection_preview():
        """Live count of photos that would match an unsaved rules list.

        Body: {"rules": [...]}. Returns {"count": N}. Used by the smart-
        collection modal to show "Matches: N photos" as the user edits
        rules. Returns 400 on malformed rules or syntactically invalid JSON.
        """
        db = get_db()
        # Parse the body manually rather than calling request.get_json(silent=True),
        # because silent=True collapses three different states into None (no
        # body, wrong Content-Type, malformed JSON), and we want malformed
        # JSON to surface as a 400 rather than be silently treated as {}.
        raw = request.get_data(cache=False)
        if not raw:
            body = {}
        else:
            try:
                body = json.loads(raw)
            except ValueError:
                return json_error("request body must be valid JSON", 400)
        # Flask returns top-level JSON lists/numbers/strings as-is, so guard
        # against `body.get(...)` raising AttributeError on non-object payloads
        # (e.g. a client posting `[]` directly).
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object", 400)
        rules = body.get("rules", [])
        try:
            count = db.count_photos_for_rules(rules)
        except ValueError as e:
            # Validation in _build_query_from_rules raises ValueError for every
            # input shape SQLite could not bind, so we don't need to catch
            # sqlite3 errors here — letting them surface as 5xx keeps real
            # backend faults (locked DB, OperationalError, etc.) from being
            # misclassified as client errors.
            return json_error(str(e), 400)
        return jsonify({"count": count})

    @blueprint.route("/api/collections/<int:collection_id>", methods=["DELETE"])
    def api_delete_collection(collection_id):
        db = get_db()
        db.delete_collection(collection_id)
        return jsonify({"ok": True})

    @blueprint.route("/api/collections/<int:collection_id>", methods=["PUT"])
    def api_update_collection(collection_id):
        """Update a collection. Body: {"name": "...", "rules": [...|group]}."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        row = db.conn.execute(
            "SELECT id, name, rules FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, db._ws_id()),
        ).fetchone()
        if not row:
            return json_error("collection not found", 404)

        updates = []
        params = []
        if "name" in body:
            name = (body.get("name") or "").strip()
            if not name:
                return json_error("name required")
            updates.append("name = ?")
            params.append(name)
        if "rules" in body:
            rules = body.get("rules")
            try:
                # Same active-model pinning as POST so the persisted
                # rules match what the save-time preview counted
                # (Codex review r3621749904).
                rules = inject_active_visual_model(rules)
                db.count_photos_for_rules(rules)
            except ValueError as e:
                return json_error(str(e), 400)
            updates.append("rules = ?")
            params.append(json.dumps(rules))
        if "visual" in body:
            try:
                visual = validate_visual_arg(body.get("visual"))
            except ValueError as e:
                return json_error(str(e), 400)
            updates.append("visual_json = ?")
            params.append(json.dumps(visual) if visual else None)
        if updates:
            params.extend([collection_id, db._ws_id()])
            db.conn.execute(
                f"UPDATE collections SET {', '.join(updates)} "
                "WHERE id = ? AND workspace_id = ?",
                params,
            )
            db.conn.commit()
        return jsonify({"ok": True})

    @blueprint.route("/api/collections/<int:collection_id>/add-photos", methods=["POST"])
    def api_collection_add_photos(collection_id):
        """Add photos to a static collection by appending to its photo_ids rule."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        if not isinstance(photo_ids, list):
            return json_error("photo_ids must be a list")
        if not photo_ids:
            return json_error("photo_ids required")
        # Non-int entries would either crash sorted() against the existing
        # int ids or persist strings that never match `p.id IN (...)` —
        # mirrors api_photos_by_ids' validation.
        for pid in photo_ids:
            if isinstance(pid, bool) or not isinstance(pid, int):
                return json_error("photo_ids must be integers")

        row = db.conn.execute(
            "SELECT rules, visual_json FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, db._ws_id()),
        ).fetchone()
        if not row:
            return json_error("Collection not found", 404)

        # Belt-and-suspenders backstop for the picker gate: a visual
        # collection stores ``rules: []`` which passes
        # ``_collection_accepts_manual_photos``, but appending to
        # ``photo_ids`` here would not touch ``visual_json``, so the
        # manually added photos would only surface on reopen if they
        # also matched the hidden visual prompt — a silent no-op
        # (Codex r3620791304). Reject the request explicitly so the UI
        # picker (which already hides these) can't be bypassed by a
        # bookmarked or hand-crafted POST.
        if row["visual_json"] is not None:
            return json_error(
                "Cannot add photos to a visual collection; open it in "
                "Browse and refine the visual clause instead.",
                400,
            )

        rules = json.loads(row["rules"])
        if not _collection_accepts_manual_photos(rules):
            return json_error("Cannot add photos to this collection", 400)

        def _find_photo_ids_rule(node):
            if isinstance(node, list):
                for child in node:
                    found = _find_photo_ids_rule(child)
                    if found is not None:
                        return found
            elif isinstance(node, dict):
                if node.get("field") == "photo_ids":
                    return node
                if "rules" in node:
                    return _find_photo_ids_rule(node.get("rules"))
            return None

        ids_rule = _find_photo_ids_rule(rules)

        if ids_rule is None:
            ids_rule = {"field": "photo_ids", "value": []}
            if isinstance(rules, list):
                rules.append(ids_rule)
            elif isinstance(rules, dict) and isinstance(rules.get("rules"), list):
                rules["rules"].append(ids_rule)
            else:
                rules = [ids_rule]

        # Merge new IDs
        existing = set(ids_rule["value"])
        for pid in photo_ids:
            existing.add(pid)
        ids_rule["value"] = sorted(existing)

        db.conn.execute(
            "UPDATE collections SET rules = ? WHERE id = ? AND workspace_id = ?",
            (json.dumps(rules), collection_id, db._ws_id()),
        )
        db.conn.commit()
        return jsonify({"ok": True, "total": len(ids_rule["value"])})

    @blueprint.route("/api/collections/<int:collection_id>/duplicate", methods=["POST"])
    def api_collection_duplicate(collection_id):
        """Duplicate a collection within the active workspace. Returns {id}."""
        db = get_db()
        try:
            new_id = db.duplicate_collection(collection_id)
        except ValueError:
            return json_error("collection not found", 404)
        return jsonify({"ok": True, "id": new_id})

    @blueprint.route("/api/collections/<int:collection_id>/photos")
    def api_collection_photos(collection_id):
        import config as cfg
        db = get_db()
        # This endpoint evaluates ``rules`` only (see
        # ``get_collection_photos`` → ``_build_collection_query``); reject
        # visual collections up front so the pipeline/review/etc. consumers
        # that hit it don't silently scope to every metadata-matching
        # photo instead of the visually-matched subset.
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), max_per_page))
        sort = request.args.get("sort", "date")
        stacks = request_bool_arg("stacks")
        # Same focused lookup ``/api/photos/query`` offers, for the same
        # reason: a collection-scoped grid that reloads around a selected
        # card (a re-sort, an undo, a folder-health refresh) has to be told
        # where that card landed. Without it Browse could only page towards
        # the card, which costs one request per page of a result set that
        # may not contain it at all. ``focus_photo_ids`` is the several-frame
        # form — any frame of a selected stack places the same card.
        focus_photo_id = request.args.get("focus_photo_id", None, type=int)
        raw_focus_ids = (request.args.get("focus_photo_ids") or "").strip()
        focus_photo_ids = []
        if raw_focus_ids:
            try:
                focus_photo_ids = [
                    int(part) for part in raw_focus_ids.split(",") if part.strip()
                ]
            except ValueError:
                return json_error(
                    "focus_photo_ids must be a comma-separated list of integers",
                    400,
                )
            if len(focus_photo_ids) > MAX_FOCUS_PHOTO_IDS:
                return json_error(
                    "focus_photo_ids must hold at most "
                    f"{MAX_FOCUS_PHOTO_IDS} integers", 400,
                )
        focus_candidates = focus_candidate_ids(focus_photo_id, focus_photo_ids)
        # Offset pagination is only self-consistent within one snapshot. A
        # commit landing between the position lookup and the page fetch can
        # move the card across the page boundary, and the response would
        # carry a ``focus_page`` whose rows omit it — clearing the very
        # selection this path exists to preserve (the reasoning
        # ``/api/photos/query`` documents at its own focused lookup).
        focus_snapshot = bool(focus_candidates) and not db.conn.in_transaction
        if focus_snapshot:
            db.conn.execute("BEGIN")
        # If the saved rules can't be resolved (e.g. an unknown field/op left
        # over from an older schema), surface a 400 instead of a 500 so
        # callers can render a real error — this is the same collection state
        # that /api/collections flags with count_error=True.
        focus_index = None
        focus_resolved_id = None
        try:
            underlying_total = db.count_collection_photos(collection_id)
            stack_cfg = db.browse_stack_settings(cfg.load()) if stacks else None
            if focus_candidates:
                found = (
                    db.query_browse_stack_position_first(
                        [], focus_candidates, sort=sort,
                        collection_id=collection_id, stack_config=stack_cfg,
                    )
                    if stacks
                    else db.query_photo_position_first(
                        [], focus_candidates, sort=sort,
                        collection_id=collection_id,
                    )
                )
                if found is not None:
                    focus_resolved_id, focus_index = found
                    page = focus_index // per_page + 1
            if stacks:
                photos = db.query_browse_stacks(
                    [], collection_id=collection_id, sort=sort,
                    page=page, per_page=per_page, stack_config=stack_cfg,
                )
                stack_totals = db.browse_stack_totals(
                    [], collection_id=collection_id, stack_config=stack_cfg,
                )
                total = stack_totals["total"]
                stack_count = stack_totals["stack_count"]
            else:
                photos = db.get_collection_photos(
                    collection_id, page=page, per_page=per_page, sort=sort,
                )
                total = underlying_total
        except ValueError as e:
            current_app.logger.exception(
                "Collection %s has unresolvable rules", collection_id
            )
            return json_error(f"collection rules cannot be resolved: {e}", 400)
        finally:
            if focus_snapshot and db.conn.in_transaction:
                db.conn.rollback()
        photo_dicts = prepare_browse_photo_dicts(db, photos)
        response = {
            "photos": photo_dicts,
            "page": page,
            "per_page": per_page,
            "total": total,
        }
        if focus_candidates:
            response["focus_index"] = focus_index
            response["focus_page"] = page
            response["focus_photo_id"] = focus_resolved_id
        if stacks:
            response["underlying_total"] = underlying_total
            response["stack_count"] = stack_count
        return jsonify(response)

    @blueprint.route("/api/collections/<int:collection_id>/photo-ids")
    def api_collection_photo_ids(collection_id):
        """Return every photo ID matching a collection.

        Accepts ``sort`` and ``stacks`` so Select-all-matching keeps the
        same insertion order the grid shows — Best Batch seed,
        burst-review order, and export preview all read the first
        ``selectedPhotos`` entry, and a date-only ordering here would
        pick a different photo whenever the grid is sorted by
        name/rating/sharpness/quality. ``stacks=true`` further emits
        each stack's cover before its hidden members so the first id is
        always the visible top-level card even when the quality-ranked
        cover isn't the stack's earliest member under the sort (Codex
        P2 on PR #1561).
        """
        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        sort = request.args.get("sort", "date")
        stacks = request_bool_arg("stacks")
        try:
            if stacks:
                photo_ids = db.get_collection_photo_ids_stacked(
                    collection_id, sort=sort,
                    stack_config=db.browse_stack_settings(cfg.load()),
                )
            else:
                photo_ids = db.get_collection_photo_ids(collection_id, sort=sort)
        except ValueError as e:
            current_app.logger.exception(
                "Collection %s has unresolvable rules", collection_id
            )
            return json_error(f"collection rules cannot be resolved: {e}", 400)
        return jsonify({"photo_ids": photo_ids, "total": len(photo_ids)})

    return blueprint
