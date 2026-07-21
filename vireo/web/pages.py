"""User-facing page routes, separated from API and service logic."""

from flask import Blueprint, redirect, render_template

pages_blueprint = Blueprint("pages", __name__)


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
    "/variants": "variants.html",
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
    "/dashboard": "stats.html",
}


def _register_template_route(path, template):
    endpoint = path.strip("/").replace("/", "_").replace("-", "_") or "index"

    def render_page():
        return render_template(template)

    pages_blueprint.add_url_rule(path, endpoint, render_page)


for _path, _template in _TEMPLATES.items():
    _register_template_route(_path, _template)


@pages_blueprint.get("/edit/<int:photo_id>")
def photo_editor(photo_id):
    del photo_id
    return render_template("photo_editor.html")


@pages_blueprint.get("/map")
def map_page():
    return render_template("map.html", active_page="map")


@pages_blueprint.get("/stats")
def legacy_stats():
    return redirect("/dashboard")


@pages_blueprint.get("/compare")
def legacy_compare():
    return redirect("/id-conflicts")
