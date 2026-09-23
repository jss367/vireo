"""User-facing page routes, separated from API and service logic.

Besides the plain template pages this owns the entry points around them:
``/`` and ``/welcome`` (which redirect based on setup state), the
``/config-defaults.js`` globals script every page loads first, and
``/favicon.ico``.
"""

import json
import os
import sys

import config as cfg
from classification_readiness import classification_readiness
from flask import (
    Blueprint,
    Response,
    redirect,
    render_template,
    request,
    send_from_directory,
)

_STATIC_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"
)


_TEMPLATES = {
    "/browse": "browse.html",
    "/best-batch": "best_batch.html",
    "/review": "review.html",
    "/edit": "photo_editor.html",
    "/lightroom": "lightroom.html",
    "/audit": "audit.html",
    "/cull": "cull.html",
    "/pipeline": "pipeline.html",
    "/pipeline/review": "pipeline_review.html",
    "/pipeline/rapid-review": "pipeline_rapid_review.html",
    "/workspace": "workspace.html",
    "/id-conflicts": "id_conflicts.html",
    "/settings": "settings.html",
    "/storage": "storage.html",
    "/shortcuts": "shortcuts.html",
    "/keywords": "keywords.html",
    "/jobs": "jobs.html",
    "/duplicates": "duplicates.html",
    "/move": "move.html",
    "/highlights": "highlights.html",
    "/life-list": "life_list.html",
    "/locations/review": "location_review.html",
    "/misses": "misses.html",
    "/logs": "logs.html",
    "/import": "import.html",
    "/card-cleanup": "card_cleanup.html",
    "/dashboard": "stats.html",
}


def _file_manager_labels():
    """Friendly OS file-manager wording for UI labels.

    Keeps Linux/Windows users from seeing macOS-only "Finder" terminology in
    menus and buttons. Keyed off the *server* platform because the reveal
    action shells out server-side (``open`` / ``explorer`` / ``xdg-open``).
    """
    if sys.platform == "darwin":
        return {
            "name": "Finder",
            "reveal": "Reveal in Finder",
            "editor_placeholder": "/Applications/Adobe Lightroom Classic/Adobe Lightroom Classic.app",
        }
    if sys.platform.startswith("win"):
        return {
            "name": "File Explorer",
            "reveal": "Show in File Explorer",
            "editor_placeholder": r"C:\Program Files\Adobe\Adobe Lightroom Classic\lightroom.exe",
        }
    return {
        "name": "file manager",
        "reveal": "Reveal in File Manager",
        "editor_placeholder": "/usr/bin/darktable",
    }


def create_pages_blueprint(get_db):
    """Build the pages blueprint.

    Only the database accessor is injected: ``/``, ``/welcome`` and
    ``/config-defaults.js`` read setup state and the effective preview size
    from the request's database. Everything else they use (config file,
    classification readiness, file-manager wording) is importable directly.
    """
    blueprint = Blueprint("pages", __name__)

    def _register_template_route(path, template):
        endpoint = path.strip("/").replace("/", "_").replace("-", "_") or "index"

        def render_page():
            return render_template(template)

        blueprint.add_url_rule(path, endpoint, render_page)

    for path, template in _TEMPLATES.items():
        _register_template_route(path, template)

    @blueprint.get("/edit/<int:photo_id>")
    def photo_editor(photo_id):
        del photo_id
        return render_template("photo_editor.html")

    @blueprint.get("/map")
    def map_page():
        return render_template("map.html", active_page="map")

    @blueprint.get("/stats")
    def legacy_stats():
        return redirect("/dashboard")

    @blueprint.get("/compare")
    def legacy_compare():
        return redirect("/id-conflicts")

    @blueprint.route("/config-defaults.js")
    def config_defaults_js():
        """Expose backend defaults to browser code without template literals.

        Templates are kept strictly Jinja-free (see
        ``test_templates_jinja_free_except_includes``), so platform-aware
        wording is injected here as ``window.*`` globals. This script is loaded
        first in ``_navbar.html``, before any inline page script runs.
        """
        from move import rsync_install_guidance

        labels = _file_manager_labels()
        preview_max_size = get_db().get_effective_config(cfg.load()).get("preview_max_size")
        return Response(
            "window.VIREO_CONFIG_DEFAULTS = "
            + json.dumps(cfg.DEFAULTS, separators=(",", ":"))
            + ";\nwindow.VIREO_FULL_PREVIEW_MAX_SIZE = "
            + json.dumps(1920 if preview_max_size is None else preview_max_size)
            + ";\nwindow.VIREO_PLATFORM = "
            + json.dumps(sys.platform)
            + ";\nwindow.VIREO_RSYNC_INSTALL = "
            + json.dumps(rsync_install_guidance())
            + ";\nwindow.VIREO_REVEAL_LABEL = "
            + json.dumps(labels["reveal"])
            + ";\nwindow.VIREO_FILE_MANAGER_NAME = "
            + json.dumps(labels["name"])
            + ";\nwindow.VIREO_EDITOR_PATH_PLACEHOLDER = "
            + json.dumps(labels["editor_placeholder"])
            + ";\n",
            mimetype="application/javascript",
            headers={"Cache-Control": "no-store"},
        )

    @blueprint.route("/")
    def index():
        # Resume onboarding until the install can actually classify, OR the
        # user explicitly finished/skipped setup. Redirecting on
        # "model downloaded" alone stranded a user who bailed after the model
        # download but before the labels step: setup_complete stays false yet
        # the model is on disk, so they'd land back in the blocked pipeline.
        if classification_readiness(get_db())["ready"] or cfg.load().get("setup_complete"):
            return redirect("/browse")
        return redirect("/welcome")

    @blueprint.route("/welcome")
    def welcome():
        if request.args.get("force"):
            return render_template("welcome.html")
        if classification_readiness(get_db())["ready"] or cfg.load().get("setup_complete"):
            return redirect("/browse")
        return render_template("welcome.html")

    @blueprint.route("/favicon.ico")
    def favicon():
        return send_from_directory(
            _STATIC_DIR,
            "favicon.png",
            mimetype="image/png",
        )

    return blueprint
