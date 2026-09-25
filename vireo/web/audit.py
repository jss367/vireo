"""Audit: catalog-versus-disk consistency checks and their repairs.

The ``/api/audit/*`` routes back the Audit page: metadata drift between the
database and XMP sidecars, orphaned catalog rows, untracked files, stray
sidecars, hash integrity, and the actions that resolve each. Scans derive
their roots server-side from the active workspace so a client can never
scope a run that is then recorded as a clean workspace-wide check.
"""

from __future__ import annotations

import logging
import os

from flask import Blueprint, jsonify, request
from metadata import scan_metadata_warning

log = logging.getLogger(__name__)


def create_audit_blueprint(
    get_db,
    json_error,
    config,
    *,
    cleanup_cached_files_for_deleted_photos,
    invalidate_missing_originals,
    trash_paths,
):
    """Build the audit blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``), read
    when a request runs rather than when the app is built.
    ``cleanup_cached_files_for_deleted_photos`` (the app's ``PhotoDeletion``)
    unlinks the thumbnails, previews and working copies of deleted photos, and
    ``invalidate_missing_originals`` (the app's ``MissingOriginals``) drops
    the missing-originals cache after the catalog changes. ``trash_paths``
    (``app._trash_paths``, late-bound) moves deleted stray sidecars to the
    Trash.
    """
    blueprint = Blueprint("audit", __name__)

    @blueprint.route("/api/audit/drift")
    def api_audit_drift():
        db = get_db()
        from audit import check_drift

        drifts = check_drift(db)
        db.record_audit_run("drift", len(drifts))
        return jsonify(drifts)

    @blueprint.route("/api/audit/orphans")
    def api_audit_orphans():
        db = get_db()
        from audit import check_orphans

        orphans = check_orphans(db)
        db.record_audit_run("orphans", len(orphans))
        return jsonify(orphans)

    def _audit_workspace_roots(db):
        """Root folder paths of the active workspace.

        Audit scans derive their roots server-side: an audit run is
        recorded as a clean workspace-wide check, so letting the client
        scope the scan with ``root`` params would let a subset (or
        empty) request certify the whole workspace. Stray ``root``
        query params are tolerated and ignored.
        """
        return [
            f["path"] for f in db.get_folder_tree() if not f["parent_id"]
        ]

    @blueprint.route("/api/audit/untracked")
    def api_audit_untracked():
        db = get_db()
        from audit import check_untracked

        untracked = check_untracked(db, _audit_workspace_roots(db))
        db.record_audit_run("untracked", len(untracked))
        return jsonify(untracked)

    @blueprint.route("/api/audit/sidecars")
    def api_audit_sidecars():
        db = get_db()
        from audit import check_stray_sidecars

        strays = check_stray_sidecars(_audit_workspace_roots(db))
        db.record_audit_run("sidecars", len(strays))
        return jsonify(strays)

    @blueprint.route("/api/audit/delete-sidecars", methods=["POST"])
    def api_audit_delete_sidecars():
        db = get_db()
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        from audit import delete_stray_sidecars

        # Client-supplied paths are untrusted; anything outside the
        # workspace roots (the same roots the sidecars check scans)
        # is refused.
        if not isinstance(paths, list):
            return json_error("paths must be a list")
        deleted = delete_stray_sidecars(
            paths, _audit_workspace_roots(db), trash_paths=trash_paths,
        )
        return jsonify({"ok": True, "deleted": deleted})

    @blueprint.route("/api/audit/integrity")
    def api_audit_integrity():
        """Current hash-verification state from the last verify run.

        Read-only: reports stored verdicts and coverage without
        re-hashing, so the audit page can render instantly. Re-hashing
        happens in the verify-hashes background job.
        """
        db = get_db()
        from audit import check_integrity

        return jsonify(check_integrity(db))

    @blueprint.route("/api/audit/accept-hash", methods=["POST"])
    def api_audit_accept_hash():
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        from audit import accept_current_hash

        accepted = accept_current_hash(db, photo_ids)
        return jsonify({"ok": True, "accepted": accepted})

    @blueprint.route("/api/audit/summary")
    def api_audit_summary():
        db = get_db()
        from audit import build_summary

        return jsonify(build_summary(db))

    @blueprint.route("/api/audit/resolve", methods=["POST"])
    def api_audit_resolve():
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_id = body.get("photo_id")
        direction = body.get("direction")
        # resolve_drift silently no-ops on any other direction value, so a
        # typo would otherwise return {"ok": true} without resolving anything.
        if direction not in ("use_db", "use_xmp"):
            return json_error("direction must be 'use_db' or 'use_xmp'")
        if isinstance(photo_id, bool) or not isinstance(photo_id, int):
            return json_error("photo_id must be an integer")
        from audit import resolve_drift

        resolve_drift(db, photo_id, direction)
        return jsonify({"ok": True})

    @blueprint.route("/api/audit/resolve-all", methods=["POST"])
    def api_audit_resolve_all():
        db = get_db()
        body = request.get_json(silent=True) or {}
        direction = body.get("direction")
        if direction not in ("use_db", "use_xmp"):
            return json_error("direction must be 'use_db' or 'use_xmp'")
        from audit import check_drift, resolve_drift

        drifts = check_drift(db)
        for d in drifts:
            resolve_drift(db, d["photo_id"], direction)
        return jsonify({"ok": True, "resolved": len(drifts)})

    @blueprint.route("/api/audit/remove-orphans", methods=["POST"])
    def api_audit_remove_orphans():
        """Drop DB rows for photos whose source file is gone from disk,
        and unlink the cached thumbnails / previews / working copies that
        were derived from them.

        Routed through ``db.delete_photos`` rather than the older
        ``audit.remove_orphans`` so all the FK-cascading cleanup
        (detections, predictions, collection rules, iNat submissions)
        runs and ``cleanup_cached_files_for_deleted_photos`` unlinks
        the on-disk derivatives. Skipping the cache cleanup is what
        leaves stale ``<id>.jpg`` thumbnails behind that then attach to
        whatever new photo later inherits the freed rowid.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        photo_ids = body.get("photo_ids", [])
        if not isinstance(photo_ids, list) or any(type(pid) is not int for pid in photo_ids):
            return json_error("photo_ids must be a list of integers")
        if not photo_ids:
            return jsonify({"ok": True, "removed": 0})

        from audit import confirmed_orphan_ids

        confirmed = confirmed_orphan_ids(db, photo_ids)
        result = db.delete_photos(confirmed) if confirmed else {"deleted": 0}
        cleanup_cached_files_for_deleted_photos(result.get("files", []))
        if result.get("deleted"):
            invalidate_missing_originals()
        return jsonify({"ok": True, "removed": result.get("deleted", 0)})

    @blueprint.route("/api/audit/import-untracked", methods=["POST"])
    def api_audit_import_untracked():
        db = get_db()
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        from audit import import_untracked

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        try:
            imported = import_untracked(
                db, paths,
                vireo_dir=vireo_dir,
                thumb_cache_dir=config["THUMB_CACHE_DIR"],
            )
        finally:
            try:
                invalidate_missing_originals()
            except Exception:
                log.exception(
                    "Failed to invalidate missing-originals cache after audit import"
                )
        # Audit import calls scanner.scan just like the standalone scan and
        # import paths. Without ExifTool the newly imported photos still lose
        # capture date, GPS, and camera info; the frontend renders any warning
        # as a toast so the user isn't told the import "succeeded" silently.
        response = {"ok": True, "imported": imported}
        metadata_warning = scan_metadata_warning()
        if metadata_warning:
            response["warning"] = metadata_warning
        return jsonify(response)

    return blueprint
