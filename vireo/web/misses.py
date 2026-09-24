"""Misses: photos the pipeline flagged as likely rejects.

The ``/api/misses/*`` routes back the Misses page: listing photos flagged
as having no subject, a clipped subject or an out-of-focus subject, previewing
and recomputing those flags under unsaved thresholds (optionally saving them
as workspace defaults), bulk-rejecting a category, and clearing one flag on a
single photo. Every route scopes its photos through the shared filter bar's
``rules``/``visual`` expression, or the legacy scalar Browse parameters.
"""

from __future__ import annotations

import json
import math

from config import settings_write_lock
from flask import Blueprint, jsonify, request
from photo_payload import attach_edit_recipes, attach_species_representatives
from services.visual_scope import inject_active_visual_model, validate_visual_arg


def create_misses_blueprint(
    get_db,
    json_error,
    *,
    resolve_visual,
):
    """Build the misses blueprint.

    ``config.settings_write_lock`` serializes workspace ``config_overrides``
    writes with the settings routes, so saving Misses thresholds as workspace
    defaults cannot interleave with another settings save.
    ``resolve_visual`` is the app's ``VisualScope.resolve``; with
    ``inject_active_visual_model`` and ``validate_visual_arg`` from
    ``services.visual_scope`` it turns the shared filter bar's rules/visual
    expression into a photo set the same way ``/api/photos/query`` does.
    It is injected because the ``VisualScope`` instance owns the per-app
    query-text embedding cache.
    """
    blueprint = Blueprint("misses", __name__)

    def _miss_threshold_config_from_body(db, body):
        """Merge Misses-page threshold overrides into effective config."""
        import config as cfg
        from misses import miss_config_from_effective

        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")

        effective = db.get_effective_config(cfg.load())
        base_pipeline = effective.get("pipeline", {})
        if not isinstance(base_pipeline, dict):
            base_pipeline = {}
        pipeline = dict(base_pipeline)
        values = miss_config_from_effective(effective)

        specs = {
            "detector_confidence": (0.0, 1.0),
            "miss_det_confidence": (0.0, 1.0),
            "miss_classifier_override_conf": (0.0, 1.01),
            "miss_bbox_area_min": (0.0, 0.2),
            "miss_oof_ratio": (0.0, 2.0),
        }
        for key, (lo, hi) in specs.items():
            if key not in body:
                continue
            try:
                value = float(body[key])
            except (TypeError, ValueError) as err:
                raise ValueError(f"{key} must be numeric") from err
            if not math.isfinite(value) or value < lo or value > hi:
                raise ValueError(f"{key} must be between {lo:g} and {hi:g}")
            values[key] = value

        if "miss_enabled" in body:
            if not isinstance(body["miss_enabled"], bool):
                raise ValueError("miss_enabled must be boolean")
            values["miss_enabled"] = body["miss_enabled"]

        pipeline["miss_enabled"] = values["miss_enabled"]
        pipeline["miss_det_confidence"] = values["miss_det_confidence"]
        if "miss_det_confidence" in body:
            values["miss_det_confidence_burst"] = round(
                values["miss_det_confidence"] * 0.60, 4
            )
        pipeline["miss_det_confidence_burst"] = values["miss_det_confidence_burst"]
        pipeline["miss_classifier_override_conf"] = values[
            "miss_classifier_override_conf"
        ]
        pipeline["miss_bbox_area_min"] = values["miss_bbox_area_min"]
        if "miss_bbox_area_min" in body:
            values["miss_bbox_area_min_singleton"] = round(
                values["miss_bbox_area_min"] * 0.40, 5
            )
        pipeline["miss_bbox_area_min_singleton"] = values[
            "miss_bbox_area_min_singleton"
        ]
        pipeline["miss_oof_ratio"] = values["miss_oof_ratio"]
        return values, pipeline, values["detector_confidence"]

    def _save_miss_threshold_overrides(db, values, pipeline):
        with settings_write_lock:
            ws = db.get_workspace(db._active_workspace_id)
            overrides = {}
            if ws and ws["config_overrides"]:
                try:
                    overrides = (
                        json.loads(ws["config_overrides"])
                        if isinstance(ws["config_overrides"], str)
                        else ws["config_overrides"]
                    )
                except (json.JSONDecodeError, TypeError):
                    overrides = {}
            if not isinstance(overrides, dict):
                overrides = {}
            existing_pipeline = overrides.get("pipeline", {})
            if not isinstance(existing_pipeline, dict):
                existing_pipeline = {}
            for key in (
                "miss_enabled",
                "miss_det_confidence",
                "miss_det_confidence_burst",
                "miss_classifier_override_conf",
                "miss_bbox_area_min",
                "miss_bbox_area_min_singleton",
                "miss_oof_ratio",
            ):
                existing_pipeline[key] = pipeline[key]
            overrides["pipeline"] = existing_pipeline
            overrides["detector_confidence"] = values["detector_confidence"]
            db.update_workspace(db._active_workspace_id, config_overrides=overrides)

    @blueprint.route("/api/misses/config")
    def api_misses_config():
        import config as cfg
        from misses import miss_config_from_effective

        db = get_db()
        return jsonify(miss_config_from_effective(db.get_effective_config(cfg.load())))

    def _attach_miss_edit_recipes(db, grouped):
        for category in ("no_subject", "clipped", "oof"):
            photos = [dict(p) for p in grouped.get(category, [])]
            attach_species_representatives(db, photos)
            attach_edit_recipes(db, photos)
            grouped[category] = photos
        return grouped

    def _miss_filter_photo_ids(db, values):
        """Resolve Misses-page filters to an intersected workspace photo set.

        The Misses UI sends the shared filter bar's ``rules``/``visual``
        expression.  The legacy scalar parameters remain accepted for old
        bookmarks and API callers, but are no longer emitted by the page.

        Returns ``(photo_ids, visual_info)``.  ``photo_ids=None`` means the
        full active-workspace scope; ``visual_info`` mirrors
        ``/api/photos/query`` so the shared bar can surface visual-search
        fallback states instead of silently widening the result set.
        """
        def value(name):
            raw = values.get(name)
            return None if raw in (None, "") else raw

        def integer(name, minimum=None, maximum=None):
            raw = value(name)
            if raw is None:
                return None
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be an integer") from None
            if minimum is not None and parsed < minimum:
                raise ValueError(f"{name} must be at least {minimum}")
            if maximum is not None and parsed > maximum:
                raise ValueError(f"{name} must be at most {maximum}")
            return parsed

        collection_id = integer("collection_id", minimum=1)

        rules_raw = value("rules")
        visual_raw = value("visual")
        if rules_raw is not None or visual_raw is not None:
            collection_ids = None
            if collection_id is not None:
                # Validate before rules/visual resolution so both paths can
                # safely use the collection as their candidate scope while
                # preserving the friendlier legacy error messages.
                valid = {c["id"]: c for c in db.get_collections()}
                if collection_id not in valid:
                    raise ValueError("collection not found")
                if valid[collection_id]["visual_json"] is not None:
                    raise ValueError(
                        "visual collections have no rules-only expansion; "
                        "open the collection from Browse instead"
                    )
                collection_ids = db.collection_photo_ids(collection_id)
            try:
                rules = (
                    json.loads(rules_raw)
                    if isinstance(rules_raw, str)
                    else rules_raw
                )
                visual_value = (
                    json.loads(visual_raw)
                    if isinstance(visual_raw, str)
                    else visual_raw
                )
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError("rules and visual must be valid JSON") from exc

            if rules is None:
                rules = []
            rules = inject_active_visual_model(rules)
            visual = validate_visual_arg(visual_value)
            visual_info = None
            photo_ids = None
            if visual is not None:
                visual_info, ordered_ids, _ = resolve_visual(
                    db, rules, visual, collection_id=collection_id,
                )
                if ordered_ids is not None:
                    photo_ids = set(ordered_ids)
            if photo_ids is None and rules:
                photo_ids = set(db.query_photo_ids(
                    rules, collection_id=collection_id,
                ))

            # ``collection_id`` can still accompany the universal expression
            # for compatibility with an old composed Misses deep link.
            if collection_id is not None:
                photo_ids = (
                    collection_ids
                    if photo_ids is None
                    else photo_ids & collection_ids
                )
            return photo_ids, visual_info

        folder_id = integer("folder_id", minimum=1)
        rating_min = integer("rating_min", minimum=1, maximum=5)
        keyword = value("keyword")
        date_from = value("date_from")
        date_to = value("date_to")
        color_label = value("color_label")
        if color_label not in (None, "red", "yellow", "green", "blue", "purple"):
            raise ValueError("invalid color_label")
        flag = value("flag")
        if flag not in (None, "none", "flagged", "rejected"):
            raise ValueError("invalid flag")

        def boolean(name):
            raw = value(name)
            if raw is None:
                return False
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in {"1", "true", "yes", "on"}

        has_browse_filters = any((
            folder_id, rating_min, keyword, date_from, date_to, color_label, flag,
        ))
        photo_ids = None
        if has_browse_filters:
            photo_ids = set(db.get_photo_ids(
                folder_id=folder_id,
                rating_min=rating_min,
                date_from=date_from,
                date_to=date_to,
                keyword=keyword,
                keyword_match_case=boolean("keyword_match_case"),
                keyword_whole_word=boolean("keyword_whole_word"),
                color_label=color_label,
                flag=flag,
            ))
        if collection_id is not None:
            # ``collection_photo_ids`` (like ``get_collection_photos``)
            # evaluates ``rules`` only. Refuse a visual-only collection
            # here so a legacy Misses API caller does not silently widen its
            # scope to every metadata match. The shared-bar page sends the
            # collection's rules + visual clause instead of collection_id.
            valid = {c["id"]: c for c in db.get_collections()}
            if collection_id not in valid:
                raise ValueError("collection not found")
            if valid[collection_id]["visual_json"] is not None:
                raise ValueError(
                    "visual collections have no rules-only expansion; "
                    "open the collection from Browse instead"
                )
            collection_ids = db.collection_photo_ids(collection_id)
            photo_ids = (
                collection_ids
                if photo_ids is None
                else photo_ids & collection_ids
            )
        return photo_ids, None

    @blueprint.route("/api/misses", methods=["GET", "POST"])
    def api_misses():
        """Return photos flagged as misses.

        GET retains the legacy scalar query parameters for bookmarks and API
        callers. POST accepts the shared filter bar's rule/visual payload,
        avoiding URL-size limits for large saved collections. With a category,
        returns {"photos": [...], "category": X}; otherwise returns all three
        category lists. ``since`` restricts results to photos whose
        miss_computed_at >= the supplied timestamp.
        """
        db = get_db()
        if request.method == "POST":
            values = request.get_json(silent=True)
            if not isinstance(values, dict):
                return json_error("request body must be a JSON object", 400)
        else:
            values = request.args
        category = values.get("category")
        since = values.get("since") or None
        try:
            photo_ids, visual_info = _miss_filter_photo_ids(db, values)
        except ValueError as e:
            return json_error(str(e), 400)
        if category is not None:
            if category not in ("no_subject", "clipped", "oof"):
                return jsonify({"error": "invalid category"}), 400
            photos = [dict(p) for p in db.list_misses(
                category=category, since=since, photo_ids=photo_ids
            )]
            attach_species_representatives(db, photos)
            attach_edit_recipes(db, photos)
            response = {"photos": photos, "category": category}
            if visual_info is not None:
                response["visual"] = visual_info
            return jsonify(response)
        grouped = {
            "no_subject": db.list_misses(category="no_subject", since=since, photo_ids=photo_ids),
            "clipped":    db.list_misses(category="clipped", since=since, photo_ids=photo_ids),
            "oof":        db.list_misses(category="oof", since=since, photo_ids=photo_ids),
        }
        grouped = _attach_miss_edit_recipes(db, grouped)
        if visual_info is not None:
            grouped["visual"] = visual_info
        return jsonify(grouped)

    @blueprint.route("/api/misses/preview", methods=["POST"])
    def api_misses_preview():
        """Return Misses-page categories using unsaved threshold overrides."""
        from misses import preview_misses_for_workspace

        db = get_db()
        body = request.get_json(silent=True) or {}
        try:
            values, pipeline, detector_confidence = (
                _miss_threshold_config_from_body(db, body)
            )
        except ValueError as e:
            return json_error(str(e))
        try:
            photo_ids, visual_info = _miss_filter_photo_ids(db, body)
        except ValueError as e:
            return json_error(str(e), 400)
        grouped = preview_misses_for_workspace(
            db, pipeline, detector_confidence=detector_confidence,
            since=body.get("since") or None, photo_ids=photo_ids,
        )
        _attach_miss_edit_recipes(db, grouped)
        grouped["config"] = values
        grouped["preview"] = True
        if visual_info is not None:
            grouped["visual"] = visual_info
        return jsonify(grouped)

    @blueprint.route("/api/misses/recompute", methods=["POST"])
    def api_misses_recompute():
        """Recompute persisted miss flags with Misses-page thresholds."""
        from misses import compute_misses_for_workspace, preview_misses_for_workspace

        db = get_db()
        body = request.get_json(silent=True) or {}
        try:
            values, pipeline, detector_confidence = (
                _miss_threshold_config_from_body(db, body)
            )
        except ValueError as e:
            return json_error(str(e))
        try:
            photo_ids, visual_info = _miss_filter_photo_ids(db, body)
        except ValueError as e:
            return json_error(str(e), 400)

        if body.get("save_defaults") is True:
            _save_miss_threshold_overrides(db, values, pipeline)

        since = body.get("since") or None
        updated = compute_misses_for_workspace(
            db, pipeline, detector_confidence=detector_confidence, since=since,
            photo_ids=photo_ids,
        )
        grouped = preview_misses_for_workspace(
            db, pipeline, detector_confidence=detector_confidence, since=since,
            photo_ids=photo_ids,
        )
        _attach_miss_edit_recipes(db, grouped)
        grouped["config"] = values
        grouped["updated"] = updated
        grouped["saved_defaults"] = body.get("save_defaults") is True
        if visual_info is not None:
            grouped["visual"] = visual_info
        return jsonify(grouped)

    @blueprint.route("/api/misses/reject", methods=["POST"])
    def api_misses_reject():
        """Set flag='rejected' on every photo currently flagged with the given
        miss category.

        Accepts an optional ``since`` ISO timestamp that mirrors the
        ``/misses?since=...`` review-window scope; when present, only
        photos whose miss_computed_at >= since are rejected, so the bulk
        action matches what the user sees on screen. Returns
        {"rejected": n, "category": ...}.

        Records a batch ``flag`` entry in ``edit_history`` so the bulk
        change is undoable and shows up in the audit log, matching the
        behavior of ``/api/batch/flag``.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        category = body.get("category")
        since = body.get("since") or None
        if category not in ("no_subject", "clipped", "oof"):
            return jsonify({"error": "invalid category"}), 400
        try:
            photo_ids, _ = _miss_filter_photo_ids(db, body)
        except ValueError as e:
            return json_error(str(e), 400)
        affected = db.bulk_reject_miss_category(
            category, since=since, photo_ids=photo_ids
        )
        if affected:
            items = [
                {"photo_id": a["photo_id"],
                 "old_value": a["old_value"],
                 "new_value": "rejected"}
                for a in affected
            ]
            for item in items:
                db.queue_flag_change_if_enabled(
                    item["photo_id"], item["new_value"], _commit=False
                )
            db.conn.commit()
            db.record_edit(
                "flag",
                f"Rejected {len(items)} miss photos (category={category})",
                "rejected",
                items,
                is_batch=True,
            )
        return jsonify({"rejected": len(affected), "category": category})

    @blueprint.route("/api/misses/<int:photo_id>/unflag", methods=["POST"])
    def api_misses_unflag(photo_id):
        """Clear the given miss-category boolean on a single photo."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        category = body.get("category")
        if category not in ("no_subject", "clipped", "oof"):
            return jsonify({"error": "invalid category"}), 400
        try:
            db.clear_miss_flag(photo_id, category)
        except ValueError:
            return jsonify({"error": "photo not in active workspace"}), 404
        return jsonify({"ok": True})

    return blueprint
