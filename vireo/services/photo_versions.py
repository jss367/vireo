"""Atomic version changes, undo, and portable sidecar serialization."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from edit_recipe import normalize_recipe
from repositories.photo_versions import VersionConflict

if TYPE_CHECKING:
    from db import Database


def queue_versions_sync(db: Database, photo_id: int) -> None:
    """Queue the entire version set so deletions also survive sidecar sync."""
    workspace = db.require_workspace_id()
    db.remove_pending_changes(photo_id, "photo_versions", workspace_id=workspace, _commit=False)
    db.queue_change(photo_id, "photo_versions", db.photo_versions.serialize(photo_id),
                    workspace_id=workspace, _commit=False)
    db.photo_versions.refresh_pending(photo_id)


def validate_recipe(recipe: Any, photo_id: int, media_dir: str) -> dict[str, Any] | None:
    import local_masks
    result = normalize_recipe(recipe)
    local = (result or {}).get("local")
    if local and not os.path.isfile(local_masks.ensure_snapshot_for_photo(media_dir, photo_id, local["mask"]["ref"])):
        raise ValueError("The saved subject mask is missing. Refresh the mask before saving this version.")
    return result


def change_version(
    db: Database, photo_id: int, version_id: str | None, body: dict[str, Any], media_dir: str,
    *, delete: bool = False,
) -> dict[str, Any] | None:
    """Serialize writers, reject stale tabs, and commit data, history and sync together."""
    db.conn.execute("BEGIN IMMEDIATE")
    try:
        if not db.photos.get(photo_id, verify_workspace=True):
            raise LookupError("Photo not found")
        repo = db.photo_versions
        old = repo.get(photo_id, version_id) if version_id else None
        if version_id and old is None:
            raise LookupError("Version not found")
        if old and body.get("revision") != old["revision"]:
            raise VersionConflict("This version changed in another editor. Reload it before saving.")
        new, description = _change_value(db, photo_id, old, body, media_dir, delete)
        key = (old or new or {})["id"]
        before = json.dumps({"id": key, "value": old})
        after = json.dumps({"id": key, "value": new})
        db.record_edit("photo_version", description, after,
                       [{"photo_id": photo_id, "old_value": before, "new_value": after}], _commit=False)
        queue_versions_sync(db, photo_id)
        db.commit()
        return new
    except BaseException:
        db.conn.rollback()
        raise


def _change_value(
    db: Database, photo_id: int, old: dict[str, Any] | None, body: dict[str, Any], media_dir: str, delete: bool,
) -> tuple[dict[str, Any] | None, str]:
    repo = db.photo_versions
    if delete and old:
        repo.delete(photo_id, old["id"])
        return None, "Deleted " + old["name"]
    if old:
        value = dict(old)
        if "recipe" in body:
            if old["kind"] == "snapshot":
                raise ValueError("Snapshots are fixed checkpoints. Restore one or create a virtual copy to edit it.")
            value["recipe"] = validate_recipe(body["recipe"], photo_id, media_dir)
        if "name" in body:
            value["name"] = repo.validate_name(body["name"])
        value["updated_at"] = datetime.now(UTC).isoformat()
        return repo.put(photo_id, value), "Updated " + value["name"]
    source = body.get("source_version_id")
    recipe = db.get_photo_edit_recipe(photo_id)
    if source:
        source_value = repo.get(photo_id, source) if isinstance(source, str) else None
        if source_value is None:
            raise LookupError("Source version not found")
        recipe = source_value["recipe"]
    # The editor may capture unsaved adjustments without changing its source version.
    recipe = validate_recipe(body.get("recipe", recipe), photo_id, media_dir)
    value = repo.create(photo_id, recipe, body.get("name"), body.get("kind", "copy"))
    return value, ("Created virtual copy " if value["kind"] == "copy" else "Saved snapshot ") + value["name"]


def export_version_options(
    db: Database, photo_ids: list[int], body: dict[str, Any], media_dir: str | None = None,
) -> dict[str, Any]:
    """Resolve a requested version before launching a worker; never fall back to the original."""
    version_id = body.get("version_id")
    if version_id is None:
        return {}
    if not isinstance(version_id, str) or len(photo_ids) != 1:
        raise ValueError("version_id requires exactly one photo")
    version = db.photo_versions.get(photo_ids[0], version_id)
    if version is None:
        raise ValueError("Version not found for the selected photo")
    if body.get("version_revision") != version["revision"]:
        raise VersionConflict("This version changed. Reload it before exporting.")
    if media_dir is not None:
        validate_recipe(version["recipe"], photo_ids[0], media_dir)
    return {"recipe_overrides": {photo_ids[0]: version["recipe"]}}
