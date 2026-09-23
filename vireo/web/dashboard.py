"""Dashboard scope and statistics.

``/api/dashboard/options`` lists the folders and collections the Dashboard
scope picker offers (flagging degraded and visual collections, which the
rules-only stats path cannot scope to); ``/api/stats`` and ``/api/coverage``
return the headline statistics and per-stage processing coverage for that
scope.
"""

from __future__ import annotations

from flask import Blueprint, jsonify
from services.visual_scope import collection_rules_state
from web.request_args import dashboard_scope_args, reject_visual_collection


def create_dashboard_blueprint(get_db, json_error):
    """Build the dashboard blueprint.

    These routes need only the database and the JSON error helper; the
    scope parsing and visual-collection guard are imported from
    ``web.request_args``.
    """
    blueprint = Blueprint("dashboard", __name__)

    @blueprint.route("/api/dashboard/options")
    def api_dashboard_options():
        """Return lightweight scope choices without running collection counts.

        Collections whose rules can't be compiled (malformed JSON or
        unresolvable fields) are marked ``degraded`` so the Dashboard scope
        picker can disable them. Selecting one would 400 both /api/stats and
        /api/coverage via ``_build_collection_query`` and leave the Dashboard
        panels wedged until the scope is reset.
        """
        db = get_db()
        folders = [dict(row) for row in db.get_folder_tree()]
        collection_rows = db.conn.execute(
            "SELECT id, name, rules, visual_json FROM collections "
            "WHERE workspace_id = ? ORDER BY name COLLATE NOCASE, id",
            (db._ws_id(),),
        ).fetchall()
        collections = []
        for row in collection_rows:
            _, degraded = collection_rules_state(db, row["rules"])
            collections.append({
                "id": row["id"],
                "name": row["name"],
                "degraded": degraded,
                # Dashboard scope path (/api/stats, /api/coverage, and the
                # Dashboard Browse drilldown link) resolves collection_id
                # through ``_build_collection_query``, which evaluates
                # ``rules`` only. A visual collection selected here would
                # silently widen to every metadata match (Codex review
                # r3620636595), so stats.html disables these picker options.
                "has_visual": row["visual_json"] is not None,
            })
        return jsonify({"folders": folders, "collections": collections})

    @blueprint.route("/api/stats")
    def api_stats():
        db = get_db()
        scope = dashboard_scope_args()
        err = reject_visual_collection(
            db, scope.get("collection_id"), json_error=json_error,
        )
        if err is not None:
            return err
        try:
            stats = db.get_dashboard_stats(**scope)
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify(stats)

    @blueprint.route("/api/coverage")
    def api_coverage():
        """Return per-stage processing coverage for the active workspace.

        ``overall`` is the workspace-wide count for each pipeline stage, and
        ``folders`` is a per-folder breakdown (one row per top-level folder
        linked to the workspace). Both share the same coverage keys.
        """
        db = get_db()
        scope = dashboard_scope_args()
        err = reject_visual_collection(
            db, scope.get("collection_id"), json_error=json_error,
        )
        if err is not None:
            return err
        try:
            return jsonify({
                "overall": db.get_coverage_stats(**scope),
                "folders": db.get_folder_coverage_stats(**scope),
            })
        except ValueError as exc:
            return json_error(str(exc), 400)

    return blueprint
