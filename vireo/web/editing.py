"""Photo-editor support endpoints that are not tied to one photo.

``/api/edit-fields`` (the adjustable recipe fields), the global edit presets
(``/api/edit-presets`` list/save/delete), and the editor's remembered crop
ratio (``/api/editor/crop-ratio``). Per-photo edit-recipe routes live with
the other ``/api/photos`` routes.
"""

from config import read_raw_config_file, settings_write_lock
from flask import Blueprint, jsonify, request


def create_editing_blueprint(get_db, json_error):
    """Build the editing blueprint.

    The crop-ratio preference is a raw read-modify-write of the user's config
    file, so it goes through ``config.read_raw_config_file`` under
    ``config.settings_write_lock`` like every other such writer.
    """
    blueprint = Blueprint("editing", __name__)

    @blueprint.route("/api/edit-fields")
    def api_edit_fields():
        from edit_batch import FIELDS
        return jsonify({"fields": FIELDS})

    @blueprint.route("/api/edit-presets", methods=["GET", "POST"])
    def api_edit_presets():
        """List or save (upsert by name) global edit presets.

        Explicit fields support partial looks, geometry, and local adjustments.
        Calls without fields retain the legacy adjustments-only behavior.
        """
        db = get_db()
        if request.method == "GET":
            return jsonify({"presets": db.list_edit_presets()})
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        recipe = body.get("recipe")
        if not isinstance(recipe, dict):
            return json_error("recipe must be a JSON object")
        try:
            preset = db.save_edit_preset(body.get("name"), recipe, fields=body.get("fields"))
        except ValueError as e:  # includes RecipeError
            return json_error(str(e))
        return jsonify({"ok": True, "preset": preset})

    @blueprint.route("/api/edit-presets/<int:preset_id>", methods=["DELETE"])
    def api_delete_edit_preset(preset_id):
        db = get_db()
        if not db.delete_edit_preset(preset_id):
            return json_error("preset not found", 404)
        return jsonify({"ok": True})

    @blueprint.route("/api/editor/crop-ratio", methods=["GET", "PUT"])
    def api_editor_crop_ratio():
        import math

        import config as cfg

        MAX_REVISION = 9007199254740991  # Number.MAX_SAFE_INTEGER

        def valid_aspect(value):
            if type(value) not in (int, float) or value <= 0:
                return False
            try:
                return math.isfinite(value)
            except OverflowError:
                return False

        def valid_revision(value):
            return type(value) is int and 0 < value <= MAX_REVISION

        def normalized_preference(stored):
            result = {"enabled": False, "aspect": None}
            if isinstance(stored, dict) and valid_revision(stored.get("revision")):
                result["revision"] = stored["revision"]
            if not isinstance(stored, dict) or stored.get("enabled") is not True:
                return result
            aspect = stored.get("aspect")
            result.update(enabled=True, aspect=aspect if valid_aspect(aspect) else None)
            return result

        if request.method == "GET":
            return jsonify(normalized_preference(cfg.load().get("editor_crop_ratio", {})))

        body = request.get_json(silent=True)
        if not isinstance(body, dict) or type(body.get("enabled")) is not bool:
            return json_error("enabled must be a boolean", status=400)
        aspect = body.get("aspect")
        if aspect is not None and not valid_aspect(aspect):
            return json_error("aspect must be a positive finite number or null", status=400)
        revision = body.get("revision")
        if "revision" in body and not valid_revision(revision):
            return json_error("revision must be a positive safe integer", status=400)
        preference = {"enabled": body["enabled"], "aspect": aspect if body["enabled"] else None}
        if revision is not None:
            preference["revision"] = revision
        with settings_write_lock:
            current = read_raw_config_file()
            stored = normalized_preference(current.get("editor_crop_ratio", {}))
            stored_revision = stored.get("revision", 0)
            # A stored revision at the safe-integer ceiling has no valid
            # successor a browser can produce, so refusing a smaller
            # revision would wedge the preference forever. Accept the
            # rollover write and reset the counter to the incoming value.
            at_ceiling = stored_revision >= MAX_REVISION
            if (revision is not None
                    and revision <= stored_revision
                    and not at_ceiling):
                return jsonify(stored)
            current["editor_crop_ratio"] = preference
            cfg.save(current)
        return jsonify(preference)

    return blueprint
