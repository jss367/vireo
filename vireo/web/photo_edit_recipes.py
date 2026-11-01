"""Per-photo edit recipes: ``/api/photos/<id>/edit-recipe`` and friends.

Get / set / clear a photo's edit recipe, compose a recipe against the current
local mask, snapshot the active mask (``/local-mask/snapshot``), the bulk
``/api/photos/edit-recipe/apply`` and ``/summary`` routes, and a photo's
edit-recipe history. The local-mask helpers only these routes use are
module-level here.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any, cast

from app_paths import media_root
from flask import Blueprint, jsonify, request
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from services.render_cache import queue_edit_recipe_sync
from web.responses import photo_not_found_error

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from services.service_types import JsonDict, JsonError
    from web.web_types import AppConfig, GetDb, InvalidatePhotoRenderCache

log = logging.getLogger(__name__)


def _create_current_local_mask_snapshot(
    db: Database, photo_id: int, *, vireo_dir: str, native_size: tuple[int, int] | None,
) -> dict[str, str]:
    """Snapshot the active immutable mask, retrying cleanup races."""
    import local_masks

    for attempt in range(3):
        variant = db.masks_features.active_variant(photo_id)
        mask_row = (
            db.masks_features.get_mask(photo_id, variant) if variant else None
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
    raise AssertionError("unreachable: the last attempt returns or raises")


def _local_mask_stale(db: Database, photo_id: int, recipe: JsonDict | None) -> bool:
    if not recipe or not recipe.get("local"):
        return False
    import local_masks
    variant = db.masks_features.active_variant(photo_id)
    mask_row = db.masks_features.get_mask(photo_id, variant) if variant else None
    return local_masks.is_stale(recipe, mask_row)


def _unique_photo_ids(raw_ids: list[object]) -> list[int] | None:
    """``raw_ids`` in order without duplicates; ``None`` if any is not an int."""
    seen: set[int] = set()
    ids: list[int] = []
    for pid in raw_ids:
        if isinstance(pid, bool) or not isinstance(pid, int):
            return None
        if pid not in seen:
            seen.add(pid)
            ids.append(pid)
    return ids


def _attach_target_local_mask(
    db: Database, pid: int, target_recipe: JsonDict, vireo_dir: str, photo: Row | JsonDict,
) -> str | None:
    """Freeze ``pid``'s own mask into ``target_recipe``; the error text on failure."""
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
        return str(e)
    target_local_mask = dict(
        target_recipe["local"].get("mask") or {}
    )
    target_local_mask.pop("corrected", None)
    target_local_mask.update(target_mask)
    target_recipe["local"]["mask"] = target_local_mask
    return None


def _compose_bulk_targets(
    db: Database, ids: list[int], visible_set: set[int], photos: dict[int, Row], old_recipes: dict[int, JsonDict],
    recipe: JsonDict, fields: list[str] | None, mode: str, has_local: bool, vireo_dir: str,
) -> tuple[list[JsonDict], list[int], list[int], dict[str, str], dict[str, JsonDict | None]]:
    """Compose and normalize the pasted recipe for every requested photo.

    Returns ``(items, applied, skipped, local_errors, applied_recipes)``;
    ``items`` are the history entries for photos whose recipe changes.
    """
    from edit_batch import compose_recipe
    from edit_recipe import RecipeError, normalize_recipe, recipe_to_json

    items: list[JsonDict] = []
    applied: list[int] = []
    skipped: list[int] = []
    local_errors: dict[str, str] = {}
    applied_recipes: dict[str, JsonDict | None] = {}
    for pid in ids:
        if pid not in visible_set:
            skipped.append(pid)
            continue
        photo: Row | JsonDict = photos.get(pid) or {}
        old_recipe = old_recipes.get(pid)
        target_recipe: JsonDict = compose_recipe(
            old_recipe, recipe, fields, mode, native_size=_recipe_source_dimensions(photo),
        ) or {}
        if has_local and target_recipe.get("local"):
            # Local adjustments reference a photo-specific mask, so each
            # target gets its OWN snapshot (frozen from its active mask);
            # the slider values copy, the mask does not. Photos without a
            # usable mask are skipped and reported, not silently given a
            # wrong mask.
            error = _attach_target_local_mask(
                db, pid, target_recipe, vireo_dir, photo,
            )
            if error is not None:
                skipped.append(pid)
                local_errors[str(pid)] = error
                continue
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
    return items, applied, skipped, local_errors, applied_recipes


def create_photo_edit_recipes_blueprint(
    get_db: GetDb,
    json_error: JsonError,
    config: AppConfig,
    *,
    invalidate_photo_render_cache: InvalidatePhotoRenderCache,
) -> Blueprint:
    """Build the per-photo edit-recipe blueprint.

    ``config`` is ``app.config`` (``THUMB_CACHE_DIR`` is read at request
    time). ``invalidate_photo_render_cache`` is the app's
    ``services.render_cache.RenderCache`` method, shared with undo/redo's
    edit-recipe replay (``web.history``).
    """
    blueprint = Blueprint("photo_edit_recipes", __name__)

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["GET"])
    def api_get_photo_edit_recipe(photo_id: int) -> ResponseReturnValue:
        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
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
    def api_create_local_mask_snapshot(photo_id: int) -> ResponseReturnValue:
        """Freeze the photo's active SAM mask into an edit-mask snapshot.

        Returns the recipe ``local.mask`` fields the editor embeds when
        saving local adjustments. Renders read only the snapshot, so the
        live mask can regenerate without silently changing committed edits.
        """
        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        try:
            mask = _create_current_local_mask_snapshot(
                db,
                photo_id,
                vireo_dir=media_root(config["THUMB_CACHE_DIR"]),
                native_size=_recipe_source_dimensions(photo),
            )
        except ValueError as e:
            return json_error(str(e))
        return jsonify({"mask": mask, "stale": False})

    @blueprint.route("/api/photos/<int:photo_id>/local-mask/correct", methods=["POST"])
    def api_correct_local_mask(photo_id: int) -> ResponseReturnValue:
        db = get_db()
        if not db.photos.get(photo_id, verify_workspace=True):
            return photo_not_found_error(legacy_error="not found")
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        import local_masks
        try:
            mask = local_masks.correct_snapshot(
                vireo_dir=media_root(config["THUMB_CACHE_DIR"]),
                photo_id=photo_id, mask=body.get("mask"), mode=body.get("mode"),
                radius=body.get("radius"), points=body.get("points"),
                softness=body.get("softness", 0), strength=body.get("strength", 1),
                optics=body.get("optics"),
            )
        except ValueError as exc:
            return json_error(str(exc))
        return jsonify({"mask": mask})

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["PUT", "POST"])
    def api_set_photo_edit_recipe(photo_id: int) -> ResponseReturnValue:
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
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        from edit_recipe import RecipeError, normalize_recipe, recipe_to_json
        try:
            normalized = normalize_recipe(recipe)
        except RecipeError as e:
            return json_error(str(e))
        local: JsonDict | None = (normalized or {}).get("local")
        if local:
            import local_masks
            snap = local_masks.snapshot_path(
                media_root(config["THUMB_CACHE_DIR"]),
                photo_id, local["mask"]["ref"],
            )
            if not os.path.exists(snap):
                return json_error(
                    "unknown edit-mask snapshot for this photo; create one "
                    "via POST /api/photos/<id>/local-mask/snapshot first"
                )
        # The recipe, its sidecar work and its undo entry are one unit; the
        # render caches are dropped once it has committed.
        with db.transaction():
            old_recipe = db.get_photo_edit_recipe(photo_id)
            try:
                old_value = recipe_to_json(old_recipe) or ""
                new_recipe = db.set_photo_edit_recipe(photo_id, recipe, verify_workspace=True)
                new_value = recipe_to_json(new_recipe) or ""
            except RecipeError as e:
                # Raised before any write, so leaving the unit commits nothing.
                return json_error(str(e))
            except ValueError as e:
                return json_error(str(e), 403)
            if old_value != new_value:
                queue_edit_recipe_sync(db, photo_id, new_value)
                db.record_edit(
                    "edit_recipe",
                    description.strip(),
                    new_value,
                    [{"photo_id": photo_id, "old_value": old_value, "new_value": new_value}],
                )
        invalidate_photo_render_cache(db, [photo_id])
        return jsonify({
            "ok": True,
            "recipe": new_recipe,
            "local_mask_stale": _local_mask_stale(db, photo_id, new_recipe),
        })

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe", methods=["DELETE"])
    def api_clear_photo_edit_recipe(photo_id: int) -> ResponseReturnValue:
        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        from edit_recipe import recipe_to_json
        with db.transaction():
            old_recipe = db.get_photo_edit_recipe(photo_id)
            old_value = recipe_to_json(old_recipe) or ""
            try:
                db.clear_photo_edit_recipe(photo_id, verify_workspace=True)
            except ValueError as e:
                return json_error(str(e), 403)
            if old_value:
                queue_edit_recipe_sync(db, photo_id, "")
                db.record_edit(
                    "edit_recipe",
                    "Cleared photo edit recipe",
                    "",
                    [{"photo_id": photo_id, "old_value": old_value, "new_value": ""}],
                )
        invalidate_photo_render_cache(db, [photo_id])
        return jsonify({"ok": True, "recipe": None})

    @blueprint.route("/api/photos/edit-recipe/summary", methods=["POST"])
    def api_photo_edit_recipe_summary() -> ResponseReturnValue:
        from edit_batch import FIELDS, get_value

        body = request.get_json(silent=True)
        ids = body.get("photo_ids") if isinstance(body, dict) else None
        if not isinstance(ids, list) or not ids or any(type(pid) is not int for pid in ids):
            return json_error("photo_ids must be a non-empty list of integers")
        db = get_db()
        visible_ids = db.photo_visibility.visible_photo_ids(ids)
        recipe_map = db.edits.get_photo_recipes(visible_ids)
        recipes = [recipe_map.get(pid) for pid in visible_ids]
        values = {}
        for field in FIELDS:
            if "min" not in field:
                continue
            # Slider values are numbers (hashable); get_value is untyped.
            amounts: set[Any] = {get_value(recipe, field["path"], field["default"]) for recipe in recipes}
            values[field["path"]] = amounts.pop() if len(amounts) == 1 else None
        return jsonify({"values": values, "count": len(recipes)})

    @blueprint.route("/api/photos/<int:photo_id>/edit-recipe/compose", methods=["POST"])
    def api_compose_photo_edit_recipe(photo_id: int) -> ResponseReturnValue:
        from edit_batch import compose_recipe
        from edit_recipe import normalize_recipe

        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
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
            if "local" in (fields or ()) and recipe.get("local"):
                mask = _create_current_local_mask_snapshot(
                    db, photo_id,
                    vireo_dir=media_root(config["THUMB_CACHE_DIR"]),
                    native_size=_recipe_source_dimensions(photo),
                )
                recipe["local"]["mask"].pop("corrected", None)
                recipe["local"]["mask"].update(mask)
                recipe = normalize_recipe(recipe) or {}
        except (ValueError, OSError) as e:
            return json_error(str(e))
        return jsonify({"recipe": recipe})

    @blueprint.route("/api/photos/edit-recipe/apply", methods=["POST"])
    def api_apply_photo_edit_recipe_bulk() -> ResponseReturnValue:
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
        from edit_batch import validate_operation

        recipe = body.get("recipe")
        fields = body.get("fields")
        mode = body.get("mode", "merge" if fields is not None else "replace")
        try:
            fields = validate_operation(recipe, fields, mode)
        except ValueError as e:
            return json_error(str(e))
        # validate_operation raised above unless ``recipe`` is an object.
        recipe = cast("JsonDict", recipe)
        raw_ids = body.get("photo_ids")
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids must be a non-empty list")
        ids = _unique_photo_ids(raw_ids)
        if ids is None:
            return json_error("photo_ids must be integers")

        from edit_recipe import RecipeError, recipe_to_json

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
        vireo_dir = media_root(config["THUMB_CACHE_DIR"])

        visible_ids = db.photo_visibility.visible_photo_ids(ids)
        visible_set = set(visible_ids)
        old_recipes = db.edits.get_photo_recipes(visible_ids)
        # Slider edits need no photo metadata. Geometry and local masks use
        # dimensions, loaded in chunks only for those operations.
        needs_dimensions = has_local or bool(fields and {"rotation", "flip"}.intersection(fields))
        photos = db.photos.get_by_ids(visible_ids, include_exif=True) if needs_dimensions else {}
        items, applied, skipped, local_errors, applied_recipes = (
            _compose_bulk_targets(
                db, ids, visible_set, photos, old_recipes, recipe, fields,
                mode, has_local, vireo_dir,
            )
        )

        if items:
            # Commit recipes, sidecar intents, and the single undo record
            # together. A failed write rolls back the whole adjustment.
            with db.transaction():
                for item in items:
                    pid = item["photo_id"]
                    db.set_photo_edit_recipe(pid, applied_recipes[str(pid)], verify_workspace=False)
                    queue_edit_recipe_sync(db, pid, item["new_value"])
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
    def api_photo_edit_history(photo_id: int) -> ResponseReturnValue:
        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")
        limit = min(max(1, request.args.get("limit", 50, type=int)), 200)
        rows = db.edit_history.recipe_history_for_photo(photo_id, limit)

        from edit_recipe import copy_recipe

        def decode_recipe(value: str) -> dict[str, int] | None:
            if value is None:
                return None
            try:
                return copy_recipe(value)
            except Exception:  # broad: a stored payload can be any malformed shape; skip the entry, not the history
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
