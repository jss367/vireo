"""Folders: the folder tree, health, detail, relocate/delete, rescan and reveal.

Every ``/api/folders*`` endpoint lives here. Workspace membership of folders
(``/api/workspaces/<id>/folders``) belongs to ``web.workspaces``; the
``/api/jobs/scan`` launchers stay with the job routes.
"""

from __future__ import annotations

import os
import subprocess
import sys

from db import Database
from flask import Blueprint, jsonify, request
from proc import no_window_kwargs
from services.local_folder import local_root_for_folder, local_root_under_folder
from services.local_workspace import folder_has_local_workspace, stage_boundary_lock
from web.background_jobs import make_background_job


def create_folders_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    *,
    build_scan_work,
    cleanup_cached_files_for_deleted_photos,
    invalidate_missing_originals,
):
    """Build the folders blueprint.

    ``get_runner`` and ``db_path`` feed the rescan route's ``background_job``.
    Injected from ``create_app`` because they are shared with routes that stay
    there:

    - ``build_scan_work``: builds the scan job's work function; the
      ``/api/jobs/scan`` launchers use it too.
    - ``cleanup_cached_files_for_deleted_photos``: removes preview/thumbnail
      files for the photo rows a folder delete cascades away (shared with
      every other photo-removal path).
    - ``invalidate_missing_originals``: drops cached Missing Originals
      payloads when folder health, paths or rows change (the cache itself is
      owned by the ``/api/photos/missing`` routes).
    """
    blueprint = Blueprint("folders", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/folders")
    def api_folders():
        db = get_db()
        # Browse pins its destructive Remove action to the workspace the
        # tree was rendered against. Fetching the tree and the active
        # workspace ID in two independent requests would let a cross-tab
        # workspace switch pair A's rows with B's id, so opt-in callers
        # get both from a single read snapshot instead of following up
        # with /api/workspaces/active (Codex review r3799038685). The
        # legacy array response is preserved for every other caller.
        include_workspace = request.args.get("with_workspace") in ("1", "true")
        if include_workspace:
            db.conn.execute("BEGIN")
            try:
                folders = [dict(f) for f in db.get_folder_tree()]
                active_workspace_id = db._ws_id()
            finally:
                db.conn.rollback()
            return jsonify(
                {"folders": folders, "active_workspace_id": active_workspace_id}
            )
        folders = db.get_folder_tree()
        return jsonify([dict(f) for f in folders])

    @blueprint.route("/api/folders/missing")
    def api_folders_missing():
        db = get_db()
        # Keep the rows and their monotonic observation marker on one read
        # snapshot so clients can compare this response with /api/browse/init
        # independent of network delivery order.
        db.conn.execute("BEGIN")
        missing = db.get_missing_folders()
        health_version = db.get_folder_health_version()
        response = jsonify([dict(f) for f in missing])
        response.headers["X-Vireo-Folder-Health-Version"] = str(health_version)
        db.conn.rollback()
        return response

    @blueprint.route("/api/folders/check-health", methods=["POST"])
    def api_folders_check_health():
        db = get_db()
        # ``check_folder_health()`` scans every folder in the DB and returns a
        # global change count, but the client needs to know whether the
        # ACTIVE workspace's missing set changed. Snapshot the workspace-
        # scoped missing IDs before and after so the client's null-baseline
        # fallback in loadMissingFolders() (fired when the modal opens before
        # any poll seeds the snapshot) can distinguish a cross-workspace
        # flip — which must NOT reset the active Browse — from a real
        # active-workspace transition that still needs to notify Browse
        # (Codex review r3686191131).
        ws_missing_before = {f["id"] for f in db.get_missing_folders()}
        changed = db.check_folder_health()
        # A folder flipping ok→missing turns every one of its photos into a
        # ghost, and missing→ok resurrects them. Either transition would
        # otherwise be masked by a ready /api/photos/missing cache until the
        # next full scan.
        if changed:
            invalidate_missing_originals()
        db.conn.execute("BEGIN")
        missing = db.get_missing_folders()
        ws_missing_after = {f["id"] for f in missing}
        health_version = db.get_folder_health_version()
        response = jsonify({
            "changed": changed,
            "workspace_changed": ws_missing_before != ws_missing_after,
            "missing": [dict(f) for f in missing],
            "folder_health_version": health_version,
        })
        db.conn.rollback()
        return response

    @blueprint.route("/api/folders/<int:folder_id>", methods=["GET"])
    def api_folder_get(folder_id):
        """Return a single folder's id, name, and path.

        Powers the folder-tree context menu's "Copy Path" action. A lean
        response on purpose: callers that want the richer tree data already
        have /api/folders for that. Scoped to the active workspace so
        absolute paths from folders hidden in this workspace don't leak.
        """
        db = get_db()
        folder = db.get_folder(folder_id)
        if not folder:
            return json_error("folder not found", 404)
        linked = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (db._active_workspace_id, folder_id),
        ).fetchone()
        if not linked:
            return json_error("folder not found", 404)
        return jsonify({
            "id": folder["id"],
            "name": folder["name"],
            "path": folder["path"],
        })

    @blueprint.route("/api/folders/<int:folder_id>/workspaces", methods=["GET"])
    def api_folder_workspaces(folder_id):
        """List workspaces in which a Browse-visible folder appears."""
        db = get_db()
        folder = db.get_folder(folder_id)
        if not folder:
            return json_error("folder not found", 404)

        # Match the folder-detail endpoint's active-workspace boundary while
        # also recognizing read-only inheritance from a recursive root. This
        # prevents callers from using hidden folder IDs to enumerate workspace
        # names without mutating the membership table during a GET.
        folder_workspaces = db.get_folder_workspaces(folder_id)
        if not any(
            workspace["id"] == db._active_workspace_id
            for workspace in folder_workspaces
        ):
            return json_error("folder not found", 404)

        workspaces = []
        for workspace in folder_workspaces:
            workspaces.append({
                "id": workspace["id"],
                "name": workspace["name"],
                "is_active": workspace["id"] == db._active_workspace_id,
                "is_root": bool(workspace["is_root"]),
            })
        return jsonify({
            "folder": {
                "id": folder["id"],
                "name": folder["name"],
                "path": folder["path"],
            },
            "workspaces": workspaces,
        })

    @blueprint.route("/api/folders/<int:folder_id>/relocate", methods=["POST"])
    def api_folder_relocate(folder_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        new_path = body.get("path", "")
        if not new_path:
            return json_error("path is required")
        if not os.path.isdir(new_path):
            return json_error("path does not exist or is not a directory")

        # Relocating a folder that a local workspace has rebased would leave
        # local_workspace_folders and the manifest pointing at a path this
        # route just moved out from under them, so a later sync/discard could
        # not restore the catalog to the source layout. The guard and the
        # subsequent write must both run under stage_boundary_lock so a stage
        # claim cannot commit between them.
        with stage_boundary_lock():
            local_root_id = local_root_for_folder(db, folder_id)
            if local_root_id is not None:
                return json_error(
                    "Cannot relocate this folder while it has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first.",
                    409,
                )
            # Staging a descendant rebases its folders.path under local-folders/,
            # so a folders.path subtree scan (as db.relocate_folder does) no
            # longer sees it — while local_folder_mappings.source_path still
            # points at the pre-relocation location. Without this check the
            # relocate would rewrite the ancestor while the manifest keeps
            # pointing at the old descendant path, so a later sync/discard
            # could not restore or publish to the new location.
            descendant_root_id = local_root_under_folder(db, folder_id)
            if descendant_root_id is not None:
                return json_error(
                    "Cannot relocate this folder while a subfolder has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first.",
                    409,
                )
            staged, owner_ws = folder_has_local_workspace(db, folder_id)
            if staged:
                return json_error(
                    f"Cannot relocate this folder — workspace {owner_ws} has it staged locally. "
                    "Switch to that workspace and sync or discard the local copy first.",
                    409,
                )

            # Capture the old path before the DB rewrite so we can rebase the
            # corresponding darktable output subdir on disk. Developed outputs
            # are nested under developed_folder_key(folder_path), so a path
            # change invalidates the old key and would silently regress export
            # to RAW until the user re-developed.
            old_row = db.conn.execute(
                "SELECT path, status FROM folders WHERE id = ?", (folder_id,)
            ).fetchone()
            old_path = old_row["path"] if old_row else ""

            try:
                cascaded = db.relocate_folder(folder_id, new_path)
            except ValueError as e:
                if old_row and old_row["status"] == "missing":
                    current_row = db.conn.execute(
                        "SELECT status FROM folders WHERE id = ?", (folder_id,)
                    ).fetchone()
                    if current_row and current_row["status"] == "ok":
                        invalidate_missing_originals()
                return json_error(str(e), 409)

        # Relocation rewrites folders.path (and can merge/delete rows via the
        # missing→existing branch). A ready /api/photos/missing cache would
        # otherwise keep offering the pre-relocation ghost rows for removal
        # even though the originals just came back online at the new path.
        invalidate_missing_originals()

        import config as cfg
        from export import relocate_developed_dir
        effective_cfg = db.get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""
        if developed_dir and old_path:
            relocate_developed_dir(developed_dir, old_path, new_path)
            for child in cascaded:
                relocate_developed_dir(
                    developed_dir, child["old_path"], child["new_path"]
                )
        return jsonify({
            "status": "ok",
            "cascaded": cascaded,
        })

    @blueprint.route("/api/folders/<int:folder_id>", methods=["DELETE"])
    def api_folder_delete(folder_id):
        db = get_db()
        # Deleting a folder that a local workspace has rebased removes the
        # folders row that local_workspace_folders and the manifest depend on,
        # so a later sync/discard would be unable to restore the catalog. The
        # guard and delete run under stage_boundary_lock so a stage claim
        # cannot commit between them; the local_workspace_folders FK to
        # folders(id) ON DELETE CASCADE is the final safety net.
        with stage_boundary_lock():
            local_root_id = local_root_for_folder(db, folder_id)
            if local_root_id is not None:
                return json_error(
                    "Cannot delete this folder while it has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first.",
                    409,
                )
            descendant_root_id = local_root_under_folder(db, folder_id)
            if descendant_root_id is not None:
                return json_error(
                    "Cannot delete this folder while a subfolder has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first.",
                    409,
                )
            staged, owner_ws = folder_has_local_workspace(db, folder_id)
            if staged:
                return json_error(
                    f"Cannot delete this folder — workspace {owner_ws} has it staged locally. "
                    "Switch to that workspace and sync or discard the local copy first.",
                    409,
                )
            result = db.delete_folder(folder_id)
        # Clean up cached files alongside the cascaded photo rows so preview
        # files don't get orphaned on disk (untracked by preview_cache).
        cleanup_cached_files_for_deleted_photos(result.get("files", []))
        # The cascaded row deletion here is the same shape as any other
        # photo-removal path: without invalidation, a ready
        # /api/photos/missing cache could keep listing ghosts from the
        # now-deleted folder (offered up for removal a second time in
        # the modal) until the next scan runs.
        invalidate_missing_originals()
        # Don't leak the internal file list to the API response — keep the
        # shape callers expect.
        return jsonify({"deleted_photos": result["deleted_photos"]})

    @blueprint.route("/api/folders/<int:folder_id>/rescan", methods=["POST"])
    @background_job
    def api_folder_rescan(ctx, folder_id):
        """Queue a scan job scoped to the given folder's path.

        Body (optional): {"incremental": bool}
        Returns: {"job_id": "scan-..."} on success; 404 if the folder id
        is unknown or not linked to the active workspace.
        """
        body = request.get_json(silent=True) or {}
        incremental = bool(body.get("incremental", False))
        db = get_db()
        folder = db.get_folder(folder_id)
        if not folder:
            return json_error("folder not found", 404)
        # Folders are global but scans emit workspace-scoped data (predictions,
        # pending_changes). Reject rescans of folders the active workspace has
        # no claim on — otherwise a stale UI or crafted request could pollute
        # this workspace with scan output from an unrelated folder, and
        # add_folder's auto-link would silently attach it.
        linked = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (ctx.workspace_id, folder_id),
        ).fetchone()
        if not linked:
            return json_error("folder not found", 404)
        root = folder["path"]
        from image_loader import is_excluded_scan_path
        # See api_job_scan for why this must run before os.path.isdir.
        if is_excluded_scan_path(root):
            return json_error(
                f"folder is inside a macOS app-managed library and cannot "
                f"be scanned: {root}"
            )
        if not os.path.isdir(root):
            return json_error(f"folder path no longer exists: {root}")

        work = build_scan_work(root, incremental, ctx.workspace_id)

        return ctx.start(
            "scan", work,
            config={
                "root": root,
                "incremental": incremental,
                "folder_id": folder_id,
            },
            pausable=True,
        )

    @blueprint.route("/api/folders/reveal", methods=["POST"])
    def api_folders_reveal():
        """Reveal a batch of folder paths in the OS file manager.

        Body: ``{"paths": [str, ...]}``. Backs the bulk-decide UI's
        "Reveal in Finder" button, which opens every folder in a bucket
        with a single click.

        Each path must exist as a row in the ``folders`` table — refusing
        arbitrary filesystem paths is the security boundary, otherwise
        a malicious caller could probe paths via the side-channel of
        whether the OS file manager opened. Unknown paths are returned
        in ``skipped`` rather than 404'd so a single bad path doesn't
        kill a bucket-wide batch.

        Returns ``{"ok": True, "revealed": [str], "skipped":
        [{"path", "reason"}], "failed": [{"path", "reason"}]}``.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("paths required")
        paths = body.get("paths")
        if not isinstance(paths, list) or not paths:
            return json_error("paths required")
        for p in paths:
            if not isinstance(p, str) or not p:
                return json_error("paths must be a list of non-empty strings")

        db = get_db()
        # Validate every path against the ``folders`` table — refusing
        # arbitrary filesystem paths is the security boundary, otherwise
        # a caller could probe disk via the OS-window-opens side channel.
        # Don't gate on workspace_folders: duplicate scans are
        # library-wide (file_hash is global), so a bucket can legitimately
        # surface folders linked only to another workspace, and Vireo is
        # a single-user app where workspaces are organizational rather
        # than access-bound.
        # Normalize both sides so legacy/relocated rows stored with a
        # trailing separator still match bucket-UI paths derived from
        # ``os.path.dirname(...)`` — same trap bulk_resolve_by_folder
        # patched.
        norm_paths = [os.path.normpath(p) for p in paths]
        all_rows = db.conn.execute("SELECT path FROM folders").fetchall()
        known_norm = {os.path.normpath(r["path"]) for r in all_rows}

        revealed = []
        skipped = []
        failed = []
        for path, norm in zip(paths, norm_paths, strict=True):
            if norm not in known_norm:
                skipped.append({"path": path, "reason": "not a known folder"})
                continue
            try:
                if sys.platform == "darwin":
                    proc = subprocess.run(["open", "-R", "--", path],
                                          timeout=5, check=False,
                                          **no_window_kwargs())
                elif sys.platform.startswith("win"):
                    # Folder reveal opens the folder itself (no /select,)
                    # so the user sees its contents.
                    proc = subprocess.run(["explorer", path],
                                          timeout=5, check=False,
                                          **no_window_kwargs())
                else:
                    # xdg-open doesn't honor `--`; abspath guarantees a
                    # leading slash so a crafted leading-dash path can't
                    # be parsed as a flag.
                    proc = subprocess.run(
                        ["xdg-open", os.path.abspath(path)],
                        timeout=5, check=False,
                        **no_window_kwargs(),
                    )
                # check=False returns a CompletedProcess for every exit
                # code; classify non-zero as failed so the UI doesn't
                # report success when nothing actually opened (e.g.
                # unmounted volume, stale path).
                if proc.returncode != 0:
                    failed.append({
                        "path": path,
                        "reason": f"reveal command exited {proc.returncode}",
                    })
                else:
                    revealed.append(path)
            except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
                failed.append({"path": path, "reason": str(exc)})

        return jsonify({
            "ok": True,
            "revealed": revealed,
            "skipped": skipped,
            "failed": failed,
        })

    return blueprint
