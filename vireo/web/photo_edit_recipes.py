"""Per-photo edit recipes: ``/api/photos/<id>/edit-recipe`` and friends.

Get / set / clear a photo's edit recipe, compose a recipe against the current
local mask, snapshot the active mask (``/local-mask/snapshot``), the bulk
``/api/photos/edit-recipe/apply`` and ``/summary`` routes, and a photo's
edit-recipe history. The local-mask helpers only these routes use are
module-level here.
"""

import json
import logging
import os

from flask import Blueprint, jsonify, request
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)

log = logging.getLogger(__name__)


def _create_current_local_mask_snapshot(
    db, photo_id, *, vireo_dir, native_size,
):
    """Snapshot the active immutable mask, retrying cleanup races."""
    import local_masks

    for attempt in range(3):
        variant_row = db.conn.execute(
            "SELECT active_mask_variant FROM photos WHERE id=?",
            (photo_id,),
        ).fetchone()
        variant = (
            variant_row["active_mask_variant"] if variant_row else None
        )
        mask_row = (
            db.get_photo_mask(photo_id, variant) if variant else None
        )
        try:
            return local_masks.create_snapshot(
                photo_id=photo_id,
                mask_row=mask_row,
                vireo_dir=vireo_dir,
                native_size=native_size,
            )
        except FileNotFoundError:
            if attempt == 2:
                raise ValueError(
                    "active subject mask changed during snapshot; retry"
                ) from None
        except ValueError as exc:
            # ``create_snapshot`` can observe a predecessor after the DB
            # lookup but after cleanup at its existence check. Retry only
            # that missing-file result; validation errors are stable.
            if (
                str(exc) != "active subject mask file is missing"
                or attempt == 2
            ):
                raise


def _local_mask_stale(db, photo_id, recipe):
    if not recipe or not recipe.get("local"):
        return False
    import local_masks
    variant_row = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    variant = variant_row["active_mask_variant"] if variant_row else None
    mask_row = db.get_photo_mask(photo_id, variant) if variant else None
    return local_masks.is_stale(recipe, mask_row)


def create_photo_edit_recipes_blueprint(
    get_db,
    json_error,
    config,
    *,
    photo_not_found_error,
    invalidate_photo_render_cache,
    queue_edit_recipe_sync,
):
    """Build the per-photo edit-recipe blueprint.

    ``config`` is ``app.config`` (``THUMB_CACHE_DIR`` is read at request
    time). ``photo_not_found_error``, ``invalidate_photo_render_cache`` and
    ``queue_edit_recipe_sync`` are injected from ``create_app`` because
    routes that stay there (undo/redo's edit-recipe replay, the per-photo
    lookups of other route groups) still call them.
    """
    blueprint = Blueprint("photo_edit_recipes", __name__)

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["GET"])
    def api_get_photo_edit_recipe(photo_id):
        db = get_db()
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        recipe = db.get_photo_edit_recipe(photo_id)
        return jsonify({
            "photo_id": photo_id,
            "recipe": recipe,
            "local_mask_stale": _local_mask_stale(db, photo_id, recipe),
        })

    @blueprint.route(
        "/api/photos/<int:photo_id>/local-mask/snapshot", methods=["POST"]
    )
    def api_create_local_mask_snapshot(photo_id):
        """Freeze the photo's active SAM mask into an edit-mask snapshot.

        Returns the recipe ``local.mask`` fields the editor embeds when
        saving local adjustments. Renders read only the snapshot, so the
        live mask can regenerate without silently changing committed edits.
        """
        db = get_db()
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        try:
            mask = _create_current_local_mask_snapshot(
                db,
                photo_id,
                vireo_dir=os.path.dirname(config["THUMB_CACHE_DIR"]),
                native_size=_recipe_source_dimensions(photo),
            )
        except ValueError as e:
            return json_error(str(e))
        return jsonify({"mask": mask, "stale": False})

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["PUT", "POST"])
    def api_set_photo_edit_recipe(photo_id):
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        recipe = body.get("recipe", body)
        description = body.get("description")
        if not isinstance(description, str) or not description.strip():
            description = "Updated photo edit recipe"
        if not isinstance(recipe, dict):
            return json_error("recipe must be a JSON object")
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        from image_edits import RecipeError, normalize_recipe, recipe_to_json
        try:
            normalized = normalize_recipe(recipe)
        except RecipeError as e:
            return json_error(str(e))
        local = (normalized or {}).get("local")
        if local:
            import local_masks
            snap = local_masks.snapshot_path(
                os.path.dirname(config["THUMB_CACHE_DIR"]),
                photo_id, local["mask"]["ref"],
            )
            if not os.path.exists(snap):
                return json_error(
                    "unknown edit-mask snapshot for this photo; create one "
                    "via POST /api/photos/<id>/local-mask/snapshot first"
                )
        old_recipe = db.get_photo_edit_recipe(photo_id)
        try:
            old_value = recipe_to_json(old_recipe) or ""
            new_recipe = db.set_photo_edit_recipe(photo_id, recipe, verify_workspace=True)
            new_value = recipe_to_json(new_recipe) or ""
        except RecipeError as e:
            return json_error(str(e))
        except ValueError as e:
            return json_error(str(e), 403)
        invalidate_photo_render_cache(db, [photo_id])
        if old_value != new_value:
            queue_edit_recipe_sync(db, photo_id, new_value)
            db.record_edit(
                "edit_recipe",
                description.strip(),
                new_value,
                [{"photo_id": photo_id, "old_value": old_value, "new_value": new_value}],
            )
        return jsonify({
            "ok": True,
            "recipe": new_recipe,
            "local_mask_stale": _local_mask_stale(db, photo_id, new_recipe),
        })

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["DELETE"])
    def api_clear_photo_edit_recipe(photo_id):
        db = get_db()
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        from image_edits import recipe_to_json
        old_recipe = db.get_photo_edit_recipe(photo_id)
        old_value = recipe_to_json(old_recipe) or ""
        try:
            db.clear_photo_edit_recipe(photo_id, verify_workspace=True)
        except ValueError as e:
            return json_error(str(e), 403)
        invalidate_photo_render_cache(db, [photo_id])
        if old_value:
            queue_edit_recipe_sync(db, photo_id, "")
            db.record_edit(
                "edit_recipe",
                "Cleared photo edit recipe",
                "",
                [{"photo_id": photo_id, "old_value": old_value, "new_value": ""}],
            )
        return jsonify({"ok": True, "recipe": None})

    @blueprint.route("/api/photos/edit-recipe/summary", methods=["POST"])
    def api_photo_edit_recipe_summary():
        from edit_batch import FIELDS, get_value

        body = request.get_json(silent=True)
        ids = body.get("photo_ids") if isinstance(body, dict) else None
        if not isinstance(ids, list) or not ids or any(type(pid) is not int for pid in ids):
            return json_error("photo_ids must be a non-empty list of integers")
        db = get_db()
        visible_ids = db.filter_photo_ids_in_workspace(ids)
        recipe_map = db.get_photo_edit_recipes(visible_ids)
        recipes = [recipe_map.get(pid) for pid in visible_ids]
        values = {}
        for field in FIELDS:
            if "min" not in field:
                continue
            amounts = {get_value(recipe, field["path"], field["default"]) for recipe in recipes}
            values[field["path"]] = amounts.pop() if len(amounts) == 1 else None
        return jsonify({"values": values, "count": len(recipes)})

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe/compose", methods=["POST"])
    def api_compose_photo_edit_recipe(photo_id):
        from edit_batch import compose_recipe
        from image_edits import normalize_recipe

        db = get_db()
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        try:
            current = normalize_recipe(body.get("current"))
            fields = body.get("fields")
            recipe = compose_recipe(
                current, body.get("recipe"), fields, "merge", native_size=_recipe_source_dimensions(photo),
            ) or {}
            if "local" in fields and recipe.get("local"):
                mask = _create_current_local_mask_snapshot(
                    db, photo_id,
                    vireo_dir=os.path.dirname(config["THUMB_CACHE_DIR"]),
                    native_size=_recipe_source_dimensions(photo),
                )
                recipe["local"]["mask"].update(mask)
        except (ValueError, OSError) as e:
            return json_error(str(e))
        return jsonify({"recipe": recipe})

    @blueprint.route("/api/photos/edit-recipe/apply", methods=["POST"])
    def api_apply_photo_edit_recipe_bulk():
        """Apply one edit recipe to many photos (copy/paste edit settings).

        Replaces or merges selected settings and records a
        single undoable batch history entry. Photos missing from the active
        workspace are skipped (reported in ``skipped``) rather than failing
        the whole request.
        """
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        from edit_batch import compose_recipe, validate_operation

        recipe = body.get("recipe")
        fields = body.get("fields")
        mode = body.get("mode", "merge" if fields is not None else "replace")
        try:
            fields = validate_operation(recipe, fields, mode)
        except ValueError as e:
            return json_error(str(e))
        raw_ids = body.get("photo_ids")
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids must be a non-empty list")
        seen = set()
        ids = []
        for pid in raw_ids:
            if isinstance(pid, bool) or not isinstance(pid, int):
                return json_error("photo_ids must be integers")
            if pid not in seen:
                seen.add(pid)
                ids.append(pid)

        from image_edits import RecipeError, normalize_recipe, recipe_to_json

        # Validate the incoming recipe once up front so a bad payload fails
        # cleanly before we touch any rows.
        try:
            target_json = json.dumps(recipe) if mode == "relative" else (recipe_to_json(recipe) or "")
        except RecipeError as e:
            return json_error(str(e))

        description = body.get("description")
        if not isinstance(description, str) or not description.strip():
            description = "Pasted edit settings"
        description = description.strip()

        has_local = bool(recipe.get("local")) and (fields is None or "local" in fields)
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        visible_ids = db.filter_photo_ids_in_workspace(ids)
        visible_set = set(visible_ids)
        old_recipes = db.get_photo_edit_recipes(visible_ids)
        # Slider edits need no photo metadata. Geometry and local masks use
        # dimensions, loaded in chunks only for those operations.
        needs_dimensions = has_local or bool(fields and {"rotation", "flip"}.intersection(fields))
        photos = db.get_photos_by_ids(visible_ids, include_exif=True) if needs_dimensions else {}
        items = []
        applied = []
        skipped = []
        local_errors = {}
        applied_recipes = {}
        for pid in ids:
            if pid not in visible_set:
                skipped.append(pid)
                continue
            photo = photos.get(pid) or {}
            old_recipe = old_recipes.get(pid)
            target_recipe = compose_recipe(
                old_recipe, recipe, fields, mode, native_size=_recipe_source_dimensions(photo),
            ) or {}
            if has_local and target_recipe.get("local"):
                # Local adjustments reference a photo-specific mask, so each
                # target gets its OWN snapshot (frozen from its active mask);
                # the slider values copy, the mask does not. Photos without a
                # usable mask are skipped and reported, not silently given a
                # wrong mask.
                try:
                    target_mask = _create_current_local_mask_snapshot(
                        db,
                        pid,
                        vireo_dir=vireo_dir,
                        native_size=_recipe_source_dimensions(photo),
                    )
                except (ValueError, OSError) as e:
                    # OSError covers disk-write failures inside create_snapshot
                    # (os.makedirs, f.write, os.replace) so a transient I/O
                    # hiccup on one target doesn't abort the whole batch.
                    skipped.append(pid)
                    local_errors[str(pid)] = str(e)
                    continue
                target_local_mask = dict(
                    target_recipe["local"].get("mask") or {}
                )
                target_local_mask.update(target_mask)
                target_recipe["local"]["mask"] = target_local_mask
            old_value = recipe_to_json(old_recipe) or ""
            try:
                new_recipe = normalize_recipe(target_recipe)
            except (RecipeError, ValueError):
                skipped.append(pid)
                continue
            new_value = recipe_to_json(new_recipe) or ""
            applied.append(pid)
            applied_recipes[str(pid)] = new_recipe
            if old_value != new_value:
                items.append({
                    "photo_id": pid,
                    "old_value": old_value,
                    "new_value": new_value,
                })

        if items:
            # Commit recipes, sidecar intents, and the single undo record
            # together. A failed write rolls back the whole adjustment.
            with db.conn:
                for item in items:
                    pid = item["photo_id"]
                    db.set_photo_edit_recipe(pid, applied_recipes[str(pid)], verify_workspace=False, _commit=False)
                    queue_edit_recipe_sync(db, pid, item["new_value"], _commit=False)
                db.record_edit("edit_recipe", description, target_json, items, is_batch=True, _commit=False)
            db._prune_edit_history()
            invalidate_photo_render_cache(db, [item["photo_id"] for item in items])
        # ``recipes`` maps each applied id to the recipe actually stored for
        # it — critical when ``has_local`` is true because each target has
        # its own mask snapshot ref, so callers cannot reuse the pasted
        # recipe (or the first applied recipe) for every id. ``recipe`` is
        # kept as the first applied recipe for backwards compatibility.
        payload = {
            "ok": True,
            "applied": applied,
            "skipped": skipped,
            "count": len(applied),
            "recipe": applied_recipes[str(applied[0])] if applied else None,
            "recipes": applied_recipes,
        }
        if local_errors:
            payload["local_errors"] = local_errors
        return jsonify(payload)

    @blueprint.route("/api/photos/<int:photo_id>/edit-history")
    def api_photo_edit_history(photo_id):
        db = get_db()
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        limit = min(max(1, request.args.get("limit", 50, type=int)), 200)
        rows = db.conn.execute(
            """SELECT eh.id, eh.description, eh.created_at, eh.undone,
                      ehi.old_value, ehi.new_value
               FROM edit_history eh
               JOIN edit_history_items ehi ON ehi.edit_id = eh.id
               WHERE eh.workspace_id = ?
                 AND eh.action_type = 'edit_recipe'
                 AND ehi.photo_id = ?
               ORDER BY eh.created_at DESC, eh.id DESC
               LIMIT ?""",
            (db._ws_id(), photo_id, limit),
        ).fetchall()

        from image_edits import copy_recipe

        def decode_recipe(value):
            if value is None:
                return None
            try:
                return copy_recipe(value)
            except Exception:
                log.warning(
                    "Skipping invalid edit recipe history payload for photo %s",
                    photo_id,
                    exc_info=True,
                )
                return None

        history = []
        for row in rows:
            history.append({
                "id": row["id"],
                "description": row["description"],
                "created_at": row["created_at"],
                "undone": bool(row["undone"]),
                "old_recipe": decode_recipe(row["old_value"]),
                "new_recipe": decode_recipe(row["new_value"]),
            })
        return jsonify({
            "photo_id": photo_id,
            "current_recipe": db.get_photo_edit_recipe(photo_id),
            "history": history,
        })

    return blueprint
