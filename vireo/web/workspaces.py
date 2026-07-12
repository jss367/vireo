"""Workspace navigation endpoints."""

from db import ALL_NAV_IDS, DEFAULT_TABS
from flask import Blueprint, jsonify, request


def create_workspace_blueprint(get_db, json_error, all_pages):
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

    return blueprint
