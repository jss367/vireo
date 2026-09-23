"""Edit history: undo, redo and the edit-history log.

``/api/undo`` and ``/api/redo`` (plus their ``/status`` probes) reverse or
replay the newest undoable ``edit_history`` entry for the active workspace;
``/api/edit-history`` pages through the log. Undo and redo are
prediction-decision routes (they restore ``prediction_review`` statuses), so
they run their check-then-write under the shared lock in
``services.prediction_decisions``.

The follow-up helpers only these routes use live here too: re-reading edit
recipes after an ``edit_recipe`` entry flips, refreshing the pipeline cache's
species fields after a species-affecting entry flips, and detecting whether
an entry changed any flags.
"""

from __future__ import annotations

import json
import logging
import os

from flask import Blueprint, jsonify, request
from services import prediction_decisions

log = logging.getLogger(__name__)


def create_history_blueprint(
    get_db,
    json_error,
    db_path,
    *,
    invalidate_photo_render_cache,
    queue_edit_recipe_sync,
):
    """Build the undo/redo and edit-history blueprint.

    ``db_path`` locates the pipeline cache directory beside the catalog.

    ``invalidate_photo_render_cache`` and ``queue_edit_recipe_sync`` are
    injected from ``create_app`` because the edit-recipe routes that stay
    there (and ``web.photo_edit_recipes``) share them: undoing or redoing an
    ``edit_recipe`` entry must drop the same render caches and queue the same
    XMP sync rows a direct recipe write does.
    """
    blueprint = Blueprint("history", __name__)

    def _edit_recipe_history_updates(db, edit_id):
        from image_edits import recipe_to_json
        rows = db.conn.execute(
            "SELECT photo_id FROM edit_history_items WHERE edit_id = ?",
            (edit_id,),
        ).fetchall()
        photo_ids = [r["photo_id"] for r in rows]
        if not photo_ids:
            return {}
        invalidate_photo_render_cache(db, photo_ids)
        updates = {}
        for photo_id in photo_ids:
            recipe = db.get_photo_edit_recipe(photo_id)
            updates[str(photo_id)] = recipe
            queue_edit_recipe_sync(
                db, photo_id, recipe_to_json(recipe) or "",
            )
        return updates

    def _refresh_pipeline_cache_species_from_edit(db, entry):
        """Rewrite the pipeline cache's species fields to match the DB after
        undo / redo of a species-affecting edit.

        The ``/api/encounters/species`` route writes the new confirmed set
        onto its target burst (or encounter) and persists the cache in the
        same request, so undo / redo of that write leaves the cache showing
        the pre-undo confirmed set even though the DB tags have flipped. Read
        the affected photos' current species from the DB and rebuild the
        cache's per-photo, per-burst, and per-encounter species fields so a
        Process / Rapid Review reload matches what ``/api/encounters/species``
        would see now.
        """
        action = entry.get("action_type") if entry else None
        if action == "pipeline_grouping":
            # A species edit whose cache write changed the encounter
            # structure is recorded as a grouping edit wrapping the photo
            # edit; the grouping restore already put bursts and encounters
            # back from its snapshot, so only the per-photo lists remain.
            try:
                payload = json.loads(entry.get("new_value") or "{}")
            except (TypeError, ValueError):
                payload = {}
            if payload.get("photo_only"):
                # Retired after its cache snapshot went stale: the newer
                # cache belongs to a later analysis and must stay untouched;
                # only the photo edit was reversed.
                return
            action = (payload.get("photo_edit") or {}).get("action_type")
        if action not in ("species_replace", "keyword_add", "prediction_accept"):
            return
        photo_id_rows = db.conn.execute(
            "SELECT DISTINCT photo_id FROM edit_history_items WHERE edit_id = ?",
            (entry["id"],),
        ).fetchall()
        photo_ids = [r["photo_id"] for r in photo_id_rows if r["photo_id"] is not None]
        if not photo_ids:
            return
        try:
            species_by_photo = db.get_species_keywords_for_photos(photo_ids)
            # Photos with no surviving species tags must still be included so
            # the cache clears their confirmed_species_list — get_species_
            # keywords_for_photos omits them entirely.
            for pid in photo_ids:
                species_by_photo.setdefault(pid, [])
            from pipeline import refresh_cache_species_for_photos
            cache_dir = os.path.dirname(db_path)
            # Photos only: burst overrides and encounter fields are owned by
            # the grouping/species-confirm history entries (restored from
            # their recorded snapshots), and a plain keyword edit never
            # changed them.
            refresh_cache_species_for_photos(
                cache_dir, db._active_workspace_id, species_by_photo,
                photos_only=True,
            )
        except Exception:
            log.exception(
                "Failed to refresh pipeline cache species after undo/redo of %s",
                action,
            )

    def _history_flags_changed(db, entry):
        action = entry["action_type"]
        if action == "pipeline_grouping":
            action = (json.loads(entry["new_value"]).get("photo_edit") or {}).get("action_type")
        if action != "flag":
            return False
        return db.conn.execute(
            "SELECT 1 FROM edit_history_items WHERE edit_id = ? AND old_value != new_value LIMIT 1",
            (entry["id"],),
        ).fetchone() is not None

    @blueprint.route("/api/undo", methods=["POST"])
    def api_undo():
        """Undo the most recent undoable edit.

        A prediction-decision route, so it takes the shared writer lock:
        ``prediction_accept`` entries restore ``prediction_review`` statuses,
        and undo is check-then-write like every other decision path — it reads
        the newest undoable entry, then rewrites the statuses that entry
        recorded. Without the lock it can restore a status on top of a batch
        decision that landed in between, and two overlapping undos can pick
        the same entry and apply it twice.
        """
        db = get_db()
        undone = []

        def _apply():
            entry = db.undo_last_edit()
            if entry is None:
                return json_error("nothing to undo")
            undone.append(entry)
            return None

        from services.grouping_history import GroupingHistoryConflict

        try:
            early = prediction_decisions.under_prediction_decision_lock(
                db, _apply, json_error=json_error,
            )
        except GroupingHistoryConflict as exc:
            return json_error(str(exc), 409)
        if early is not None:
            return early
        result = undone[0]
        edit_recipe_updates = None
        if result.get("action_type") == "edit_recipe":
            # Deliberately outside the locked section: this queues XMP sync
            # rows and commits them itself, and it touches no prediction
            # state, so it has nothing to serialize against.
            edit_recipe_updates = _edit_recipe_history_updates(db, result["id"])
        _refresh_pipeline_cache_species_from_edit(db, result)
        response = {"ok": True, "undone": result["description"], "flags_changed": _history_flags_changed(db, result)}
        if edit_recipe_updates is not None:
            response["action_type"] = "edit_recipe"
            response["edit_recipes"] = edit_recipe_updates
        return jsonify(response)

    @blueprint.route("/api/undo/status")
    def api_undo_status():
        db = get_db()
        from db import Database
        non_undoable = Database._NON_UNDOABLE
        placeholders = ",".join("?" for _ in non_undoable)
        latest = db.conn.execute(
            f"SELECT id, description FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (db._ws_id(), *non_undoable),
        ).fetchone()
        if not latest:
            return jsonify({"available": False, "description": "", "count": 0})
        total = db.conn.execute(
            f"SELECT COUNT(*) FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders})",
            (db._ws_id(), *non_undoable),
        ).fetchone()[0]
        return jsonify({
            "available": True,
            "description": latest["description"],
            "id": latest["id"],
            "count": total,
        })

    @blueprint.route("/api/redo", methods=["POST"])
    def api_redo():
        """Redo the most recently undone edit.

        Under the prediction decision lock for the reason ``api_undo`` gives.
        """
        db = get_db()
        redone = []

        def _apply():
            entry = db.redo_last_undo()
            if entry is None:
                return json_error("nothing to redo")
            redone.append(entry)
            return None

        from services.grouping_history import GroupingHistoryConflict

        try:
            early = prediction_decisions.under_prediction_decision_lock(
                db, _apply, json_error=json_error,
            )
        except GroupingHistoryConflict as exc:
            return json_error(str(exc), 409)
        if early is not None:
            return early
        result = redone[0]
        edit_recipe_updates = None
        if result.get("action_type") == "edit_recipe":
            # Outside the locked section, for the reason ``api_undo`` gives.
            edit_recipe_updates = _edit_recipe_history_updates(db, result["id"])
        _refresh_pipeline_cache_species_from_edit(db, result)
        response = {"ok": True, "redone": result["description"], "flags_changed": _history_flags_changed(db, result)}
        if edit_recipe_updates is not None:
            response["action_type"] = "edit_recipe"
            response["edit_recipes"] = edit_recipe_updates
        return jsonify(response)

    @blueprint.route("/api/redo/status")
    def api_redo_status():
        db = get_db()
        from db import Database
        non_undoable = Database._NON_UNDOABLE
        placeholders = ",".join("?" for _ in non_undoable)
        latest = db.conn.execute(
            f"SELECT id, description FROM edit_history WHERE workspace_id = ? AND undone = 1 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at ASC, id ASC LIMIT 1",
            (db._ws_id(), *non_undoable),
        ).fetchone()
        if not latest:
            return jsonify({"available": False, "description": ""})
        return jsonify({
            "available": True,
            "description": latest["description"],
        })

    @blueprint.route("/api/edit-history")
    def api_edit_history():
        db = get_db()
        limit = min(max(1, request.args.get("limit", 50, type=int)), 1000)
        offset = max(0, request.args.get("offset", 0, type=int))
        return jsonify(db.get_edit_history(limit=limit, offset=offset))

    return blueprint
