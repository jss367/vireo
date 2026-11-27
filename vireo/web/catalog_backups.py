"""Catalog backups: the Settings section's list, Back up now, and Restore.

``GET /api/catalog-backups`` lists the backups with the schedule and any
staged or finished restore. ``POST /api/catalog-backups`` starts a backup;
``POST /api/catalog-backups/<name>/restore`` starts the job that prepares a
restore, which Vireo finishes at its next launch; ``DELETE
/api/catalog-backups/pending-restore`` drops a prepared restore. The work
lives in ``services.catalog_backups``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Blueprint, jsonify
from services.catalog_backups import BackupBusy, list_backups

if TYPE_CHECKING:
    from flask.typing import ResponseReturnValue
    from services.catalog_backups import CatalogBackups
    from services.service_types import JsonError
    from web.web_types import GetDb


def create_catalog_backups_blueprint(
    get_db: GetDb, json_error: JsonError, *, catalog_backups: CatalogBackups,
) -> Blueprint:
    """Build the catalog-backups blueprint.

    ``get_db`` is unused: the backups are files beside the catalog, read
    without a catalog connection. It is taken to keep the factory shape.
    """
    del get_db
    blueprint = Blueprint("catalog_backups", __name__)

    @blueprint.route("/api/catalog-backups")
    def api_catalog_backups() -> ResponseReturnValue:
        return jsonify(catalog_backups.status())

    @blueprint.route("/api/catalog-backups", methods=["POST"])
    def api_catalog_backup_now() -> ResponseReturnValue:
        job_id, joined = catalog_backups.start_backup(kind="manual")
        return jsonify({"job_id": job_id, "joined": joined})

    @blueprint.route("/api/catalog-backups/<name>/restore", methods=["POST"])
    def api_catalog_backup_restore(name: str) -> ResponseReturnValue:
        backup = next((b for b in list_backups(catalog_backups.db_path) if b["name"] == name), None)
        if backup is None:
            return json_error(f"There is no backup named {name!r}.", status=404)
        if not backup["restorable"]:
            return json_error(backup["not_restorable_reason"], status=409)
        return jsonify({"job_id": catalog_backups.start_restore(name)})

    @blueprint.route("/api/catalog-backups/pending-restore", methods=["DELETE"])
    def api_catalog_backup_cancel_restore() -> ResponseReturnValue:
        from services.catalog_backups import cancel_restore

        try:
            cancelled = cancel_restore(catalog_backups.db_path)
        except BackupBusy as exc:
            return json_error(str(exc), status=409)
        return jsonify({"cancelled": cancelled})

    return blueprint
