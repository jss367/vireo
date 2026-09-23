"""Workspaces: CRUD, activation, folder membership, config and new-images.

Every ``/api/workspaces/*`` endpoint owned by the core app lives here, plus the
``/api/workspace/tabs/*`` navigation endpoints. The local-workspace and
local-folder staging routes under ``/api/workspaces/active/...`` have their own
blueprints (``web.local_workspace`` / ``web.local_folder``).
"""

from __future__ import annotations

import json
import logging
import os

from db import ALL_NAV_IDS, DEFAULT_TABS
from flask import Blueprint, abort, jsonify, request
from services.local_folder import (
    affected_workspace_ids as local_folder_workspace_ids,
)
from services.local_folder import (
    local_root_for_folder,
    local_roots_under_folder,
    workspace_local_root_ids,
)
from services.local_workspace import (
    folder_has_local_workspace,
    has_local_workspace,
    stage_boundary_lock,
)
from web.settings import (
    LOCATION_KEYWORDS_SETTING,
    queue_location_keyword_cleanup_for_workspace,
    workspace_effective_setting,
)

log = logging.getLogger(__name__)

# How long a request that *starts* a new-images walk waits for it before
# answering ``pending``. Small libraries finish inside this window and
# get a real count synchronously. Kept under the 0.5s slow-request
# threshold so a normal cold probe never logs as a slow request.
_NEW_IMAGES_SYNC_WAIT_SECS = 0.4


def create_workspace_blueprint(
    get_db,
    json_error,
    all_pages,
    *,
    get_runner,
    invalidate_missing_originals,
    settings_write_lock,
    new_images_walk_progress,
    missing_originals_heavy_job_types,
):
    """Build the workspaces blueprint.

    Injected from ``create_app`` because they are shared with routes that stay
    there:

    - ``get_runner``: returns the app's ``JobRunner`` (new-images walks are
      recorded jobs, and walks defer while storage moves or heavy jobs run).
    - ``invalidate_missing_originals``: drops cached Missing Originals payloads
      when workspace membership changes or a workspace id is created/deleted.
    - ``settings_write_lock``: the settings write lock, so the curated
      workspace config and subject-types writes can't race a schema-driven
      settings autosave.
    - ``new_images_walk_progress``: the ``app._new_images_walk_progress`` dict,
      so pending new-images responses report live walk totals.
    - ``missing_originals_heavy_job_types``: job types that defer automatic
      background walks (shared with the Missing Originals scan).
    """
    blueprint = Blueprint("workspaces", __name__)

    def _nav_id():
        body = request.get_json(silent=True) or {}
        nav_id = body.get("nav_id")
        if not isinstance(nav_id, str):
            return None, json_error("nav_id must be a string", 400)
        if nav_id not in ALL_NAV_IDS:
            return None, json_error("nav_id is not a known page", 400)
        return nav_id, None

    @blueprint.post("/api/workspace/tabs/pin")
    def pin_tab():
        nav_id, error = _nav_id()
        if error:
            return error
        return jsonify({"ok": True, "tabs": get_db().pin_tab(nav_id)})

    @blueprint.post("/api/workspace/tabs/unpin")
    def unpin_tab():
        nav_id, error = _nav_id()
        if error:
            return error
        return jsonify({"ok": True, "tabs": get_db().unpin_tab(nav_id)})

    @blueprint.post("/api/workspace/tabs/reorder")
    def reorder_tabs():
        body = request.get_json(silent=True) or {}
        tabs = body.get("tabs")
        if not isinstance(tabs, list):
            return json_error("tabs must be a list", 400)
        try:
            result = get_db().set_tabs(tabs)
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify({"ok": True, "tabs": result})

    @blueprint.get("/api/workspace/tabs")
    def get_tabs():
        db = get_db()
        try:
            tabs = db.get_tabs()
        except Exception:
            tabs = list(DEFAULT_TABS)
        return jsonify({
            "tabs": tabs,
            "all_pages": all_pages,
            "navigation_migrated": db.get_meta("navigation_consolidated") == "1",
        })

    @blueprint.route("/api/workspaces")
    def api_get_workspaces():
        db = get_db()
        workspaces = db.get_workspaces()
        return jsonify([dict(w) for w in workspaces])

    @blueprint.route("/api/workspaces/active")
    def api_get_active_workspace():
        db = get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if not ws:
            return json_error("No active workspace", 404)
        result = dict(ws)
        # Navigation only needs workspace identity. Folder photo counts can
        # take seconds on large libraries and must not delay the switcher.
        if request.args.get("include_folders") != "0":
            result["folders"] = [dict(f) for f in db.get_workspace_folder_roots(ws["id"])]
        return jsonify(result)

    def _validate_workspace_config_overrides(overrides, db):
        """Validate ``config_overrides`` before persist. Returns an error
        response (from ``json_error``) on rejection, or None on success.

        Shared by POST /api/workspaces and PUT /api/workspaces/<id> so a
        workspace created with a bogus ``pipeline.default_process_id`` can't
        later feed the import/process settings or chaining hook a nonexistent
        process — the update path already rejects it and the create path
        must too, otherwise the create endpoint becomes a validation bypass.
        """
        # Anything else would be persisted as-is and crash the labels
        # accessors that expect a JSON object (or NULL) in this column.
        if overrides is not None and not isinstance(overrides, dict):
            return json_error("config_overrides must be an object or null")
        # workspace_effective_setting() reads this override with bool(), so a
        # stored string like "false" would resolve to True and the PUT
        # True -> False transition check would never queue the XMP keyword
        # cleanup. Require a real boolean (or null / omitted). This is
        # stricter than config_schema.validate_value, which coerces strings.
        location_keywords = (overrides or {}).get(LOCATION_KEYWORDS_SETTING)
        if location_keywords is not None and not isinstance(location_keywords, bool):
            return json_error(
                f"{LOCATION_KEYWORDS_SETTING} must be a boolean or null"
            )
        pipeline_overrides = (overrides or {}).get("pipeline")
        # Translate the legacy ``pipeline.default_strategy`` (hardcoded strategy
        # name) to ``pipeline.default_process_id`` before existence checks.
        # An older client (or a workspace payload restored from a pre-migration
        # backup) that still sends the legacy key would otherwise fall through
        # this validator — non-schema pipeline keys are stored as-is — and
        # `get_effective_config()` would then see `default_process_id: null`,
        # so imports read only the new key and silently run import-only
        # instead of the requested after-import process. Mirror the
        # /api/settings/import translation exactly: unknown/removed names map
        # to null (import only). If both keys are present the new one wins
        # and the legacy key is dropped so it can't resurface on a later read.
        if (
            isinstance(pipeline_overrides, dict)
            and "default_strategy" in pipeline_overrides
        ):
            import process_strategies as ps

            legacy = pipeline_overrides.pop("default_strategy")
            if "default_process_id" not in pipeline_overrides:
                seed_name = (
                    ps.LEGACY_STRATEGY_NAMES.get(legacy)
                    if isinstance(legacy, str) else None
                )
                translated_pid = None
                if seed_name is not None:
                    match = next(
                        (
                            p for p in db.get_saved_processes()
                            if p["name"] == seed_name
                        ),
                        None,
                    )
                    if match is not None:
                        translated_pid = match["id"]
                pipeline_overrides["default_process_id"] = translated_pid
        # pipeline.default_process_id: None means "no automatic processing
        # after import" (the chaining hook short-circuits on it) and is
        # accepted as-is; a non-null value must name a real saved_processes
        # row so the chaining hook never fails hours later on a dangling id.
        if (
            isinstance(pipeline_overrides, dict)
            and pipeline_overrides.get("default_process_id") is not None
        ):
            pid = pipeline_overrides["default_process_id"]
            if not isinstance(pid, int) or isinstance(pid, bool):
                return json_error("default_process_id must be an integer or null")
            if db.get_saved_process(pid) is None:
                return json_error(f"unknown process id: {pid}")
        return None

    @blueprint.route("/api/workspaces", methods=["POST"])
    def api_create_workspace():
        db = get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return json_error("Name is required")
        config_overrides = body.get("config_overrides")
        err = _validate_workspace_config_overrides(config_overrides, db)
        if err is not None:
            return err
        folder_ids = body.get("folder_ids", [])
        # A folder already covered by another workspace's
        # local_workspace_folders row points at that workspace's managed local
        # copy, not the original NAS path. Silently linking it into a
        # brand-new workspace would make imports and edits here share the
        # managed copy, so the owning workspace's later sync could publish
        # them or discard could delete them. This mirrors the same check that
        # POST /api/workspaces/<id>/folders already runs; without it, an API
        # client can bypass the guard by supplying folder_ids at create time.
        # The check + create + link run under stage_boundary_lock so a stage
        # claim cannot slip between "guard says free" and add_workspace_folder.
        try:
            with stage_boundary_lock():
                for folder_id in folder_ids:
                    staged_by, owner_ws = folder_has_local_workspace(db, folder_id)
                    if staged_by:
                        return json_error(
                            f"Folder is staged locally by workspace {owner_ws}. "
                            "Sync or discard that workspace's local copy before linking the folder here.",
                            409,
                        )
                ws_id = db.create_workspace(name, config_overrides=config_overrides)
                # SQLite can reuse the rowid of a deleted workspace here, so a
                # ready Missing Originals payload cached under the old
                # workspace could otherwise be served to this fresh workspace
                # until its own scan overwrites the entry — mirroring the
                # new-images cache guard in db.create_workspace.
                invalidate_missing_originals(workspace_ids=[ws_id])
                # Seed the standard smart collections (All Photos, Flagged, etc.).
                # Startup only seeds the active workspace, so without this a
                # workspace created via the API never gets defaults until it's
                # active during a future Vireo restart — which is how a
                # workspace in the wild ended up with zero defaults and broke
                # "All Photos" in the pipeline collection picker.
                db.create_default_collections(workspace_id=ws_id)
                # Link selected folders if provided
                for folder_id in folder_ids:
                    db.add_workspace_folder(ws_id, folder_id)
                db.mark_workspace_folder_roots(ws_id, folder_ids)
                ws = db.get_workspace(ws_id)
            return jsonify(dict(ws))
        except Exception as e:
            return json_error(str(e))

    @blueprint.route("/api/workspaces/<int:ws_id>", methods=["PUT"])
    def api_update_workspace(ws_id):
        db = get_db()
        existing_ws = db.get_workspace(ws_id)
        if not existing_ws:
            return json_error("Workspace not found", 404)
        body = request.get_json(silent=True) or {}
        kwargs = {}
        if "name" in body:
            kwargs["name"] = body["name"]
        overrides_changing = "config_overrides" in body
        if overrides_changing:
            overrides = body["config_overrides"]
            err = _validate_workspace_config_overrides(overrides, db)
            if err is not None:
                return err
            kwargs["config_overrides"] = overrides
        if "ui_state" in body:
            kwargs["ui_state"] = body["ui_state"]
        # A full workspace update can silently swap the ``config_overrides``
        # object, including its own override for ``write_location_keywords_to_xmp``.
        # The per-key PATCH and DELETE endpoints already run the effective-value
        # transition check that queues cleanup on a True → False flip; the
        # bulk PUT must too, otherwise the located photos in this workspace
        # keep their Vireo-written keywords and markers in XMP indefinitely.
        prev_effective_location_keywords = None
        if overrides_changing:
            import config as cfg
            global_cfg = cfg.load()
            prev_effective_location_keywords = workspace_effective_setting(
                existing_ws["config_overrides"],
                global_cfg,
                LOCATION_KEYWORDS_SETTING,
            )
        db.update_workspace(ws_id, **kwargs)
        ws = db.get_workspace(ws_id)
        if overrides_changing and prev_effective_location_keywords:
            new_effective = workspace_effective_setting(
                ws["config_overrides"],
                global_cfg,
                LOCATION_KEYWORDS_SETTING,
            )
            if not new_effective:
                queue_location_keyword_cleanup_for_workspace(db, ws_id)
        return jsonify(dict(ws))

    @blueprint.route("/api/workspaces/<int:ws_id>", methods=["DELETE"])
    def api_delete_workspace(ws_id):
        db = get_db()
        # Prevent deleting the last workspace
        workspaces = db.get_workspaces()
        if len(workspaces) <= 1:
            return json_error("Cannot delete the only workspace")
        # Prevent deleting the active workspace
        if ws_id == db._active_workspace_id:
            return json_error("Cannot delete the active workspace. Switch first.")
        # Guard and delete run under stage_boundary_lock so a stage claim
        # cannot commit between the check and the workspace DELETE.
        with stage_boundary_lock():
            if has_local_workspace(db, ws_id):
                return json_error(
                    "Cannot delete a workspace with local work. Switch to it and sync or discard the local copy first.",
                    409,
                )
            # Only block when this workspace is the last remaining link to a
            # local root. If other workspaces still share the local copy,
            # deleting this workspace is equivalent to unlinking one non-final
            # workspace_folders row — which the folder-unlink route already
            # permits and delete_workspace cascades the same way.
            for root_id in workspace_local_root_ids(db, ws_id):
                if local_folder_workspace_ids(db, root_id) == [ws_id]:
                    return json_error(
                        "This workspace is the last one linked to a local folder. "
                        "Sync or discard its local copy before deleting the workspace.",
                        409,
                    )
            try:
                db.delete_workspace(ws_id)
            except ValueError as e:
                return json_error(str(e), 409)
        # Drop this workspace's cached Missing Originals payload so a
        # later workspace that reuses this SQLite rowid can't be served
        # the deleted workspace's ghost photos / folder paths.
        invalidate_missing_originals(workspace_ids=[ws_id])
        return jsonify({"ok": True})

    @blueprint.route("/api/workspaces/<int:ws_id>/activate", methods=["POST"])
    def api_activate_workspace(ws_id):
        db = get_db()
        ws = db.get_workspace(ws_id)
        if not ws:
            return json_error("Workspace not found", 404)
        from datetime import datetime

        # Save current page path to the outgoing workspace's ui_state
        body = request.get_json(silent=True) or {}
        current_path = body.get("current_path")
        if current_path and db._active_workspace_id:
            old_ws = db.get_workspace(db._active_workspace_id)
            if old_ws:
                try:
                    ui = json.loads(old_ws["ui_state"]) if old_ws["ui_state"] else {}
                except (json.JSONDecodeError, TypeError):
                    ui = {}
                ui["last_path"] = current_path
                db.update_workspace(db._active_workspace_id, ui_state=ui)

        # Activate the new workspace
        db.set_active_workspace(ws_id)
        db.create_default_collections(workspace_id=ws_id)
        db.update_workspace(ws_id, last_opened_at=datetime.now().isoformat())

        # Return the target workspace's saved page path
        restore_path = None
        if ws["ui_state"]:
            try:
                ui = json.loads(ws["ui_state"]) if isinstance(ws["ui_state"], str) else ws["ui_state"]
                restore_path = ui.get("last_path")
            except (json.JSONDecodeError, TypeError):
                pass

        return jsonify({"ok": True, "workspace": dict(ws), "restore_path": restore_path})

    @blueprint.route("/api/workspaces/<int:ws_id>/pin", methods=["POST"])
    def api_pin_workspace(ws_id):
        db = get_db()
        ws = db.get_workspace(ws_id)
        if not ws:
            return json_error("Workspace not found", 404)
        body = request.get_json(silent=True) or {}
        pinned = body.get("pinned", True)
        if not isinstance(pinned, bool):
            return json_error("`pinned` must be a boolean")
        from datetime import datetime
        db.update_workspace(
            ws_id,
            pinned_at=datetime.now().isoformat() if pinned else None,
        )
        return jsonify(dict(db.get_workspace(ws_id)))

    @blueprint.route("/api/workspaces/<int:ws_id>/folders", methods=["GET"])
    def api_workspace_folders(ws_id):
        db = get_db()
        folders = db.get_workspace_folder_roots(ws_id)
        return jsonify([dict(f) for f in folders])

    @blueprint.route("/api/workspaces/<int:ws_id>/folders", methods=["POST"])
    def api_add_workspace_folder(ws_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        folder_id = body.get("folder_id")
        if not folder_id:
            return json_error("folder_id is required")
        # Pre-check both sides of the link — an unknown id would otherwise
        # hit the workspace_folders FK and 500.
        if not db.get_workspace(ws_id):
            return json_error("Workspace not found", 404)
        if not db.get_folder(folder_id):
            return json_error("Folder not found", 404)
        # A workspace with active local state has folder paths rebased into
        # the managed copy and a manifest covering those paths. Silently
        # adding a folder here would leave the UI showing a folder that the
        # manifest and local_workspace_folders don't cover, so a later sync
        # or discard could not act on it consistently. Guards and INSERT run
        # under stage_boundary_lock so a stage claim cannot commit between
        # them.
        with stage_boundary_lock():
            if has_local_workspace(db, ws_id):
                return json_error(
                    "Cannot change folder membership while working locally. Sync or discard the local copy first.",
                    409,
                )
            # A folder already covered by another workspace's
            # local_workspace_folders points at that workspace's managed copy,
            # not the original NAS path. Linking it into a second workspace
            # would silently make edits or imports there share the managed
            # copy, so the owning workspace's later sync could publish them
            # to the source and discard could delete them. Refuse until the
            # owning workspace's local copy is resolved.
            staged_by, owner_ws = folder_has_local_workspace(db, folder_id)
            if staged_by:
                return json_error(
                    f"Folder is staged locally by workspace {owner_ws}. "
                    "Sync or discard that workspace's local copy before linking the folder here.",
                    409,
                )
            db.add_workspace_folder(ws_id, folder_id)
        # A newly linked folder can introduce ghosts (or resolve them if it
        # was previously offline). The missing-originals cache is keyed by
        # workspace, so leaving a stale ready payload here would keep serving
        # the old membership's answer until the next scan.
        invalidate_missing_originals()
        return jsonify({"ok": True})

    @blueprint.route("/api/workspaces/<int:ws_id>/folders/<int:folder_id>", methods=["DELETE"])
    def api_remove_workspace_folder(ws_id, folder_id):
        db = get_db()
        # Pre-check the workspace — an unknown id would otherwise hit the
        # workspace_folder_removals FK when we record the tombstone and 500.
        if not db.get_workspace(ws_id):
            return json_error("Workspace not found", 404)
        # Removing a staged root while local work is active would leave
        # local_workspace_folders and the manifest covering paths the UI no
        # longer shows, so a later sync could publish/delete files for a
        # folder the workspace has already dropped. Guard and DELETE run
        # under stage_boundary_lock so a stage claim cannot commit between
        # them.
        with stage_boundary_lock():
            if has_local_workspace(db, ws_id):
                return json_error(
                    "Cannot change folder membership while working locally. Sync or discard the local copy first.",
                    409,
                )
            local_root_id = local_root_for_folder(db, folder_id)
            if local_root_id is not None:
                linked_workspaces = local_folder_workspace_ids(db, local_root_id)
                if linked_workspaces == [ws_id]:
                    return json_error(
                        "This is the last workspace linked to a local folder. "
                        "Sync or discard its local copy before removing it.",
                        409,
                    )
            # Ancestor case: the folder being unlinked is not itself a staged
            # root, but a descendant local session lives beneath it. Staging
            # rebased the descendant's folders.path under local-folders/, so
            # remove_workspace_folder_tree()'s folders.path subtree walk misses
            # it and would leave a hidden non-root workspace_folders row that
            # still contributes to affected_workspace_ids. Refuse when this
            # workspace is the last remaining link (matches the exact-folder
            # branch above); otherwise sweep the descendant session's rows
            # ourselves after the standard subtree unlink.
            descendant_root_ids = local_roots_under_folder(db, folder_id)
            for descendant_id in descendant_root_ids:
                linked_workspaces = local_folder_workspace_ids(db, descendant_id)
                if linked_workspaces == [ws_id]:
                    return json_error(
                        "This workspace is the last one linked to a subfolder's "
                        "local copy. Sync or discard the local copy before "
                        "removing this folder.",
                        409,
                    )
            db.remove_workspace_folder_tree(ws_id, folder_id)
            for descendant_id in descendant_root_ids:
                rows = db.conn.execute(
                    "SELECT folder_id FROM local_folder_mappings WHERE root_folder_id = ?",
                    (descendant_id,),
                ).fetchall()
                for row in rows:
                    db.conn.execute(
                        "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                        (ws_id, int(row["folder_id"])),
                    )
            if descendant_root_ids:
                db.conn.commit()
        # Unlinking a folder tree removes photos from the workspace's scope;
        # the cached ready payload would otherwise keep listing ghosts from
        # the now-detached folders until a manual rescan.
        invalidate_missing_originals()
        return jsonify({"ok": True})

    @blueprint.route("/api/workspaces/<int:ws_id>/move-folders", methods=["POST"])
    def api_move_workspace_folders(ws_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        folder_ids = body.get("folder_ids", [])
        target_ws_id = body.get("target_workspace_id")
        new_ws_name = (body.get("new_workspace_name") or "").strip()

        if not folder_ids:
            return json_error("folder_ids is required")
        if not target_ws_id and not new_ws_name:
            return json_error("Provide target_workspace_id or new_workspace_name")
        if target_ws_id and new_ws_name:
            return json_error("Provide target_workspace_id or new_workspace_name, not both")

        # Block folder-membership mutation while either side is working
        # locally: moving a folder off a staged workspace would leave its
        # manifest covering a folder the UI no longer shows, and moving one
        # onto a workspace whose paths are rebased would leave the new folder
        # untracked by the manifest. Guards and mutation run under
        # stage_boundary_lock so a stage claim on either side cannot commit
        # between check and write.
        with stage_boundary_lock():
            if has_local_workspace(db, ws_id):
                return json_error(
                    "Cannot move folders out of a workspace working locally. Sync or discard the local copy first.",
                    409,
                )
            if target_ws_id and has_local_workspace(db, target_ws_id):
                return json_error(
                    "Cannot move folders into a workspace working locally. Sync or discard the local copy first.",
                    409,
                )

            # Match the folder-unlink route: block moves that would abandon a
            # local session and, for the ancestor case, remember descendant
            # roots so we can sweep their rebased workspace_folders rows over
            # to the target after the move. move_folders_to_workspace uses a
            # folders.path subtree walk, so a rebased descendant hides from
            # it and would otherwise be orphaned on the source workspace.
            descendant_root_ids_by_folder: dict[int, list[int]] = {}
            for fid in folder_ids:
                local_root_id = local_root_for_folder(db, fid)
                if local_root_id is not None:
                    linked_workspaces = local_folder_workspace_ids(db, local_root_id)
                    if linked_workspaces == [ws_id]:
                        return json_error(
                            "This is the last workspace linked to a local folder. "
                            "Sync or discard its local copy before moving it.",
                            409,
                        )
                descendant_ids = local_roots_under_folder(db, fid)
                descendant_root_ids_by_folder[fid] = descendant_ids
                for descendant_id in descendant_ids:
                    linked_workspaces = local_folder_workspace_ids(db, descendant_id)
                    if linked_workspaces == [ws_id]:
                        return json_error(
                            "This workspace is the last one linked to a subfolder's "
                            "local copy. Sync or discard the local copy before "
                            "moving this folder.",
                            409,
                        )

            # Validate source workspace and folder ownership before creating a
            # new workspace to avoid orphans if the move would fail.
            if new_ws_name:
                if not db.get_workspace(ws_id):
                    return json_error(f"Source workspace {ws_id} not found")
                source_folder_ids = {f["id"] for f in db.get_workspace_folders(ws_id)}
                for fid in folder_ids:
                    if fid not in source_folder_ids:
                        return json_error(f"Folder {fid} does not belong to source workspace {ws_id}")
                from db import _path_for_subtree_match

                source_folders = db.get_workspace_folders(ws_id)
                selected = set(folder_ids)
                remaining_paths = [_path_for_subtree_match(f["path"]) for f in source_folders
                                   if f["id"] not in selected and f["path"]]
                if any(_path_for_subtree_match(f["path"]).startswith(parent + "/")
                       for f in source_folders if f["id"] in selected and f["path"]
                       for parent in remaining_paths):
                    return json_error(
                        "Cannot move a folder that is covered by another source workspace folder; "
                        "move the covering folder or remove it first"
                    )
                try:
                    target_ws_id = db.create_workspace(new_ws_name)
                except Exception as e:
                    return json_error(f"Failed to create workspace: {e}")

            try:
                result = db.move_folders_to_workspace(ws_id, target_ws_id, folder_ids)
                result["target_workspace_id"] = target_ws_id
                # Transfer the rebased descendant rows the subtree walk missed
                # so the target workspace can reach the shared local session
                # through its new ancestor root and the source no longer keeps
                # a hidden link to it.
                swept = False
                for descendant_ids in descendant_root_ids_by_folder.values():
                    for descendant_id in descendant_ids:
                        rows = db.conn.execute(
                            "SELECT folder_id FROM local_folder_mappings WHERE root_folder_id = ?",
                            (descendant_id,),
                        ).fetchall()
                        for row in rows:
                            mapped_fid = int(row["folder_id"])
                            db.conn.execute(
                                "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                                (ws_id, mapped_fid),
                            )
                            db.conn.execute(
                                """INSERT OR IGNORE INTO workspace_folders
                                       (workspace_id, folder_id, is_root)
                                   VALUES (?, ?, 0)""",
                                (target_ws_id, mapped_fid),
                            )
                            swept = True
                if swept:
                    db.conn.commit()
                # Moving folders changes membership on both source and target
                # workspaces, so any cached missing-originals payloads for either
                # side would go stale.
                invalidate_missing_originals()
                return jsonify(result)
            except ValueError as e:
                return json_error(str(e))

    @blueprint.route("/api/workspaces/active/config")
    def api_workspace_config():
        """Get the active workspace's config overrides."""
        db = get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if not ws:
            return jsonify({})
        overrides = {}
        if ws["config_overrides"]:
            try:  # noqa: SIM105 (moved verbatim from app.py)
                overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except Exception:
                pass
        return jsonify(overrides)

    @blueprint.route("/api/workspaces/active/config", methods=["POST"])
    def api_set_workspace_config():
        """Set config overrides for the active workspace."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        # Only allow workspace-overridable keys
        allowed = {"classification_threshold", "grouping_window_seconds", "similarity_threshold", "detector_confidence", "review_min_confidence"}
        # Share the schema-driven settings write lock so an autosave in the
        # All-settings region can't race with a curated workspace-form save
        # and silently drop a recent override.
        with settings_write_lock:
            ws = db.get_workspace(db._active_workspace_id)
            existing = {}
            if ws and ws["config_overrides"]:
                try:  # noqa: SIM105 (moved verbatim from app.py)
                    existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except Exception:
                    pass
            if not isinstance(existing, dict):
                existing = {}
            for k, v in body.items():
                if k in allowed:
                    if v is None:
                        existing.pop(k, None)
                    else:
                        existing[k] = v
            db.update_workspace(db._active_workspace_id, config_overrides=existing if existing else None)
        return jsonify({"ok": True, "overrides": existing})

    @blueprint.route("/api/workspaces/active/subject-types", methods=["GET"])
    def api_get_active_subject_types():
        """Return the active workspace's effective subject_types — global
        defaults merged with workspace overrides. The workspace settings UI
        needs this rather than just the override JSON, so checkboxes render
        the actual current state when the user has only customized at the
        global config layer."""
        db = get_db()
        types = sorted(db.get_subject_types())
        return jsonify({"types": types})

    @blueprint.route("/api/workspaces/<int:ws_id>/subject-types", methods=["PUT"])
    def api_set_subject_types(ws_id):
        """Set the subject_types config override for a workspace.

        Unknown type values are dropped (logged). An empty list is allowed
        (logged warning) and effectively disables the 'identified' filter.
        """
        from db import KEYWORD_TYPES
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_types = body.get("types")
        if not isinstance(raw_types, list):
            return json_error("types must be a list")
        # Guard the membership test against non-string entries (e.g. nested
        # lists or objects). `x in frozenset` raises TypeError on unhashable
        # input — that would 500 the request instead of dropping the bad
        # element per the documented "unknown values are dropped" contract.
        cleaned = [t for t in raw_types if isinstance(t, str) and t in KEYWORD_TYPES]
        dropped = [t for t in raw_types if not isinstance(t, str) or t not in KEYWORD_TYPES]
        if dropped:
            log.warning(
                "subject-types: dropped unknown values %s for ws=%s",
                dropped, ws_id,
            )
        if not cleaned:
            log.warning("subject-types: empty list set for workspace %s", ws_id)
        # Share the schema-driven settings write lock so a concurrent
        # autosave on the same workspace can't read this same overrides
        # snapshot and overwrite our subject_types change with stale data.
        with settings_write_lock:
            ws = db.get_workspace(ws_id)
            if not ws:
                return json_error("workspace not found", 404)
            existing = {}
            if ws["config_overrides"]:
                try:
                    existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except Exception:
                    existing = {}
            # `config_overrides` is JSON, so a previous PUT /api/workspaces/<id>
            # could have stored a list/string/number. Coerce to {} before key
            # assignment to keep this endpoint from 500-ing on malformed state.
            if not isinstance(existing, dict):
                existing = {}
            existing["subject_types"] = cleaned
            db.update_workspace(ws_id, config_overrides=existing)
        return jsonify({"types": cleaned})

    def _new_images_walk_fns(db, ws_id):
        """Return ``(compute, on_spawn, spawned)`` for a transparent background
        new-images walk, shared by the navbar probe and the snapshot POST.

        ``spawned()`` reports whether the most recent ``kickoff_compute`` that
        received this ``on_spawn`` actually started a worker. Only that
        request should block briefly for a result: while a multi-minute walk
        over a NAS is in flight, every follow-up poll (the client re-polls
        every 3s) used to sit out the full grace period too, so a single walk
        produced hundreds of "slow request" warnings and tied up a Flask
        worker per tab for nothing — the answer was always going to be
        ``pending``.

        The walk always runs with ``sample_limit=None`` so the cached result
        carries the complete list of new-file paths. That is what lets a
        banner click turn straight into a snapshot of exactly the files the
        banner advertised, instead of forcing a second multi-minute re-walk
        of the whole library. Memory cost is one path string per *new* file,
        which is small even for a full re-import.

        ``on_spawn`` surfaces the walk as a recorded job in the bottom
        panel and mirrors its progress into ``new_images_walk_progress``
        so pending API responses can report live totals.
        """
        from db import Database
        from new_images import count_new_images_for_workspace

        db_path = db._db_path
        # Shared holder for the cache worker's exception (if any), read by
        # the recorded job's work_fn after the cache event fires. Without
        # this, a walk that raises (e.g. unreadable volume, DB error) would
        # still mark the job ``completed`` while ``/api/.../new-images``
        # returns the error from ``get_recent_error`` — contradictory state
        # in the bottom panel.
        walk_error = {"exc": None}
        walk_result = {"result": None}
        spawn_state = {"spawned": False}

        def compute(progress_callback=None):
            # Close explicitly: this connection lives for the whole walk
            # (minutes on large volumes), and connections left to __del__
            # accumulate and exhaust the per-process fd limit.
            wdb = Database(db_path)
            try:
                wdb.set_active_workspace(ws_id)
                result = count_new_images_for_workspace(
                    wdb, ws_id, sample_limit=None,
                    progress_callback=progress_callback,
                )
                walk_result["result"] = result
                return result
            except Exception as e:
                walk_error["exc"] = e
                raise
            finally:
                wdb.close()

        # Surface the walk as a recorded job so the user can see it in the
        # bottom panel rather than wondering why their workspace is silent.
        # ``on_spawn`` only fires when this kickoff actually starts a new
        # worker (cache truly cold) — cache hits and reuse of an in-flight
        # walk skip job creation, so navbar polls don't clutter the list.
        ws_row = db.get_workspace(ws_id)
        ws_name = ws_row["name"] if ws_row else f"workspace #{ws_id}"
        runner = get_runner()

        def on_spawn(spawn_event):
            spawn_state["spawned"] = True
            progress_state = {"checked": 0, "found": 0}
            new_images_walk_progress[(db_path, ws_id)] = progress_state

            def job_work_fn(job):
                # Mirror the cache worker's lifecycle. ``spawn_event`` fires
                # in the worker's finally clause, so we wake when the walk
                # ends regardless of success or failure. Final totals come
                # from progress_state, which the cache worker populated via
                # progress_callback. If the walk raised, re-raise the same
                # exception so JobRunner marks the job ``failed`` with the
                # original message — keeping the bottom panel and the
                # ``/api/.../new-images`` payload in agreement.
                spawn_event.wait()
                if walk_error["exc"] is not None:
                    raise walk_error["exc"]
                result = walk_result["result"] or {}
                payload = {
                    "files_checked": progress_state["checked"],
                    "new_count": progress_state["found"],
                }
                # An offline volume is a completed walk with a caveat, not
                # a failure: name the roots that were skipped so the Jobs
                # panel tells the same story as the banner.
                unreachable = result.get("unreachable_roots") or []
                if unreachable:
                    payload["unreachable_roots"] = list(unreachable)
                    payload["phase"] = (
                        f"{len(unreachable)} folder(s) offline, not checked"
                    )
                return payload

            job_id = runner.start(
                "new_images_walk",
                job_work_fn,
                ephemeral=False,
                counts_for_badge=True,
                workspace_id=ws_id,
                config={"workspace_name": ws_name},
            )

            def progress_callback(files_checked, new_found):
                progress_state["checked"] = files_checked
                progress_state["found"] = new_found
                runner.push_event(
                    job_id,
                    "progress",
                    {
                        "current": files_checked,
                        "total": 0,
                        "phase": (
                            f"{files_checked:,} checked, {new_found:,} new"
                        ),
                        "files_checked": files_checked,
                        "new_count": new_found,
                    },
                )

            return progress_callback

        return compute, on_spawn, (lambda: spawn_state["spawned"])

    def _new_images_walk_progress_fields(db_path, ws_id):
        """Live totals of the current/most recent walk for pending payloads."""
        progress = new_images_walk_progress.get((db_path, ws_id)) or {}
        return {
            "files_checked": progress.get("checked", 0),
            "new_count_so_far": progress.get("found", 0),
        }

    def _new_images_walk_blocked_by_move():
        """True while a storage move owns the source/archive filesystem.

        The navbar probe is automatic and can otherwise start a whole-library
        walk seconds after a move begins.  On SMB/NAS libraries that doubles
        metadata/read pressure at exactly the point rsync is building its
        file list.  Return a deferred pending response instead; the existing
        client poll starts the walk after the move reaches a terminal state.
        """
        for job in get_runner().list_jobs():
            if job.get("status") not in (
                "queued", "running", "pausing", "paused",
            ):
                continue
            if job.get("type") in ("move-folder", "move-photos"):
                return True
        return False

    def _new_images_deferred_reason(*, manual=False):
        if _new_images_walk_blocked_by_move():
            return "storage_move_active"
        if manual:
            return None
        for job in get_runner().list_jobs():
            if (job.get("status") in ("running", "pausing", "paused", "queued")
                    and job.get("type") in missing_originals_heavy_job_types - {"new_images_walk"}):
                return "foreground_job_active"
        return None

    @blueprint.route("/api/workspaces/active/new-images")
    def api_workspace_new_images():
        db = get_db()
        ws_id = db._active_workspace_id
        if ws_id is None:
            return jsonify({"workspace_id": None, "new_count": 0, "per_root": [], "sample": []})

        def response_payload(result):
            """Return a small client payload while keeping full paths in cache."""
            payload = dict(result)
            payload["workspace_id"] = ws_id
            sample = payload.get("sample") or []
            payload["sample"] = sample[:5]
            payload.pop("sample_complete", None)
            return payload

        # The "Check again" button's follow-up GET carries this marker so
        # the deferral that hides automatic polls behind foreground work
        # doesn't also swallow a click the user just made. Without it, a
        # recheck during a long pipeline would leave the button stuck on
        # "Checking..." for the remainder of the job.
        manual_recheck = request.args.get("manual_recheck", "").lower() in ("1", "true", "yes")

        cache = db._new_images_cache
        db_path = db._db_path
        cached = cache.get(db_path, ws_id)
        if cached is not None:
            return jsonify(response_payload(cached))

        # If a recent compute failed and we're still inside the backoff
        # window, surface the error instead of kicking off another walk.
        # Without this, the navbar's pending re-poll keeps hammering a
        # broken volume / DB and the UI is stuck "checking" forever.
        recent_err = cache.get_recent_error(db_path, ws_id)
        if recent_err is not None:
            return jsonify({
                "workspace_id": ws_id,
                "new_count": None,
                "per_root": [],
                "sample": [],
                "error": recent_err,
            })

        deferred_reason = _new_images_deferred_reason(manual=manual_recheck)
        if deferred_reason and not cache.has_inflight(db_path, ws_id):
            return jsonify({
                "workspace_id": ws_id,
                "new_count": None,
                "per_root": [],
                "sample": [],
                "pending": True,
                "deferred_reason": deferred_reason,
            })

        # Cache cold: run the filesystem walk in a background thread so the
        # navbar's poll doesn't tie up a Flask worker for seconds while
        # os.walk grinds through a large library. Wait briefly so small
        # libraries (and the test suite's tmp_path filesystems) still
        # observe a real count synchronously; longer walks return
        # ``pending: true`` and the front-end re-polls.
        compute, on_spawn, spawned = _new_images_walk_fns(db, ws_id)
        event = cache.kickoff_compute(
            db_path, ws_id, compute, on_spawn=on_spawn,
            can_start=lambda: _new_images_deferred_reason(manual=manual_recheck) is None,
        )
        if spawned() and event.wait(timeout=_NEW_IMAGES_SYNC_WAIT_SECS):
            cached = cache.get(db_path, ws_id)
            if cached is not None:
                return jsonify(response_payload(cached))
            # Compute finished but produced no cached entry — it must have
            # raised. Surface the error captured by the worker.
            recent_err = cache.get_recent_error(db_path, ws_id)
            if recent_err is not None:
                return jsonify({
                    "workspace_id": ws_id,
                    "new_count": None,
                    "per_root": [],
                    "sample": [],
                    "error": recent_err,
                })

        return jsonify({
            "workspace_id": ws_id,
            "new_count": None,
            "per_root": [],
            "sample": [],
            "pending": True,
            **_new_images_walk_progress_fields(db_path, ws_id),
        })

    @blueprint.route("/api/workspaces/active/new-images/recheck", methods=["POST"])
    def api_workspace_new_images_recheck():
        """User-initiated "Check again" from the offline banner.

        The automatic path already recovers on its own — a result that
        skipped an offline root is cached for only 30s and the reachability
        gate re-probes on the same cadence — but the user cannot see either
        timer. Someone who just remounted the share and is looking at a
        banner saying their folders are offline needs a way to say "look
        now". Drop the cached walk (and its error backoff) for this
        workspace plus every cached volume verdict; the client's follow-up
        poll then starts a genuinely fresh walk.
        """
        import new_images
        import volume_reachability

        db = get_db()
        ws_id = db._active_workspace_id
        if ws_id is None:
            return jsonify({"workspace_id": None, "rechecked": False})
        volume_reachability.invalidate_caches()
        # An offline answer can also come from the walk-side watchdog rather
        # than a volume probe, and that registry is separate: without this a
        # root whose walk wedged would be reported offline again without the
        # remounted share ever being touched.
        new_images.forget_stalled_walks()
        db.invalidate_new_images_cache_for_workspace(ws_id)
        return jsonify({"workspace_id": ws_id, "rechecked": True})

    @blueprint.route("/api/workspaces/active/new-images/snapshot", methods=["POST"])
    def api_workspace_new_images_snapshot_create():
        db = get_db()
        ws_id = db._active_workspace_id
        if ws_id is None:
            return jsonify({"error": "no active workspace"}), 400
        cache = db._new_images_cache
        db_path = db._db_path

        def create_snapshot_from_result(result):
            file_paths = list(result.get("sample") or [])
            snap_id = db.create_new_images_snapshot(file_paths)
            folders = sorted({os.path.dirname(p) for p in file_paths})
            return jsonify({
                "snapshot_id": snap_id,
                "file_count": len(file_paths),
                "folders": folders,
            })

        # Trust a live cached walk. The banner count the user just clicked
        # came from exactly this cache entry (walks always run with
        # ``sample_limit=None``), so snapshotting it means the pipeline
        # covers precisely the set the banner advertised — and the click
        # resolves instantly instead of forcing a multi-minute re-walk of
        # the whole library. Files that land on disk after that walk are
        # simply picked up by a later probe and the banner reappears:
        # self-healing, and honest about what was processed. Scans and
        # imports invalidate this cache, so a stale entry can't survive the
        # very actions that consume new images.
        cached = cache.get(db_path, ws_id)
        if cached is not None and cached.get("sample_complete"):
            return create_snapshot_from_result(cached)

        recent_err = cache.get_recent_error(db_path, ws_id)
        if recent_err is not None:
            return jsonify({"error": recent_err}), 500

        if _new_images_walk_blocked_by_move():
            return jsonify({
                "pending": True,
                "deferred_reason": "storage_move_active",
                **_new_images_walk_progress_fields(db_path, ws_id),
            }), 202

        # Cache cold (e.g. a scan just invalidated it): kick off — or
        # coalesce onto — a background walk. ``kickoff_compute`` reuses an
        # in-flight walk for the current generation, so repeated clicks and
        # poll loops can never stack walks or restart one mid-flight.
        # ``on_spawn`` registers the same recorded bottom-panel job the
        # navbar probe gets, so a click-triggered walk is just as visible.
        compute, on_spawn, spawned = _new_images_walk_fns(db, ws_id)
        event = cache.kickoff_compute(
            db_path, ws_id, compute, on_spawn=on_spawn,
            can_start=lambda: _new_images_deferred_reason(manual=True) is None,
        )
        if spawned() and event.wait(timeout=_NEW_IMAGES_SYNC_WAIT_SECS):
            cached = cache.get(db_path, ws_id)
            if cached is not None and cached.get("sample_complete"):
                return create_snapshot_from_result(cached)
            recent_err = cache.get_recent_error(db_path, ws_id)
            if recent_err is not None:
                return jsonify({"error": recent_err}), 500

        # Still walking. Report live progress so the client can show
        # "N files checked, M new so far" instead of an opaque spinner.
        return jsonify({
            "pending": True,
            **_new_images_walk_progress_fields(db_path, ws_id),
        }), 202

    @blueprint.route(
        "/api/workspaces/active/new-images/snapshot/<int:snapshot_id>",
        methods=["GET"],
    )
    def api_workspace_new_images_snapshot_get(snapshot_id):
        db = get_db()
        if db._active_workspace_id is None:
            abort(404)
        snap = db.get_new_images_snapshot(snapshot_id)
        if snap is None:
            abort(404)
        paths = snap["file_paths"]
        folder_paths = sorted({os.path.dirname(p) for p in paths})
        files_sample = paths[:5]
        return jsonify({
            "file_count": snap["file_count"],
            "folder_paths": folder_paths,
            "files_sample": files_sample,
        })

    return blueprint
