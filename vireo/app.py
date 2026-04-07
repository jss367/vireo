"""Flask web app for the Vireo photo browser.

Usage:
    python vireo/app.py --db ~/.vireo/vireo.db [--port 8080]
"""

import argparse
import json
import logging
import logging.handlers
import os
import queue
import subprocess
import time
import webbrowser
from pathlib import Path

from db import Database
from flask import (
    Flask,
    Response,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
)
from highlights import select_highlights
from jobs import JobRunner, LogBroadcaster

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# File logging with rotation — persists across restarts
_log_dir = os.path.expanduser("~/.vireo")
os.makedirs(_log_dir, exist_ok=True)
_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(_log_dir, "vireo.log"),
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
)
_file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logging.getLogger().addHandler(_file_handler)


# Suppress noisy werkzeug request logs for polling endpoints
class _QuietRequestFilter(logging.Filter):
    """Filter out repetitive GET requests from werkzeug logs."""

    _quiet_paths = {"/api/jobs", "/api/logs/stream", "/api/logs/recent", "/thumbnails/"}

    def filter(self, record):
        msg = record.getMessage()
        if "200" in msg or "304" in msg:
            for path in self._quiet_paths:
                if f"GET {path}" in msg:
                    return False
        return True


logging.getLogger("werkzeug").addFilter(_QuietRequestFilter())


def _trash_via_finder(filepath):
    """Trash a file via Finder using AppleScript.

    Fallback for when send2trash fails (e.g. external volumes where the
    legacy Carbon API can't locate .Trashes).
    """
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set posixPath to item 1 of argv",
            "-e", "set fileRef to POSIX file posixPath",
            "-e", "tell application \"Finder\" to delete fileRef",
            "-e", "end run",
            "--",
            filepath,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise OSError(result.stderr.strip() or f"Finder trash failed ({result.returncode})")


def create_app(db_path, thumb_cache_dir=None):
    """Create the Flask app for the Vireo photo browser.

    Args:
        db_path: path to the SQLite database
        thumb_cache_dir: path to thumbnail cache directory
    """
    app = Flask(
        __name__, template_folder=os.path.join(os.path.dirname(__file__), "templates")
    )
    app.config["DB_PATH"] = db_path
    app.config["THUMB_CACHE_DIR"] = thumb_cache_dir or os.path.expanduser(
        "~/.vireo/thumbnails"
    )

    # Request timing middleware — logs slow requests and user actions
    @app.before_request
    def _start_timer():
        request._start_time = time.time()

    @app.after_request
    def _log_requests(response):
        if hasattr(request, "_start_time"):
            elapsed = time.time() - request._start_time
            if request.method in ("POST", "DELETE"):
                # Log user actions with details about what changed
                body = request.get_json(silent=True) or {}
                detail = ""
                path = request.path
                if "/rating" in path:
                    detail = f" rating={body.get('rating')}"
                elif "/flag" in path:
                    detail = f" flag={body.get('flag')}"
                elif "/keywords" in path and request.method == "POST":
                    detail = f" keyword={body.get('name')}"
                elif "/accept" in path:
                    detail = " (accept prediction)"
                elif "/reject" in path:
                    detail = " (reject prediction)"
                elif "batch" in path:
                    ids = body.get("photo_ids", [])
                    detail = f" ({len(ids)} photos)"
                elif "/classify" in path:
                    detail = f" collection={body.get('collection_id')}"
                elif "/scan" in path:
                    detail = f" root={body.get('root', '')}"
                log.info(
                    "Action: %s %s → %s (%.1fs)%s",
                    request.method,
                    path,
                    response.status_code,
                    elapsed,
                    detail,
                )
            elif elapsed > 0.5:
                log.warning(
                    "Slow request: %s %s took %.1fs",
                    request.method,
                    request.path,
                    elapsed,
                )
            if request.path.startswith("/api/"):
                _quiet = request.method == "GET" and request.path == "/api/jobs"
                (log.debug if _quiet else log.info)(
                    "API: %s %s → %s (%.3fs)",
                    request.method,
                    request.path,
                    response.status_code,
                    elapsed,
                )
        return response

    # Catch uncaught exceptions so they don't disappear silently
    @app.errorhandler(Exception)
    def _handle_error(e):
        from werkzeug.exceptions import HTTPException
        if isinstance(e, HTTPException):
            return e
        log.exception("Unhandled error: %s %s", request.method, request.path)
        return jsonify({"error": "Internal server error"}), 500

    _MAX_PER_PAGE = 500

    def json_error(msg, status=400):
        """Return a JSON error response. Standard shape: {"error": "msg"}."""
        return jsonify({"error": msg}), status

    def _get_db():
        """Get a Database instance. One connection per request via Flask g."""
        if "db" not in g:
            g.db = Database(db_path)
        return g.db

    @app.teardown_appcontext
    def _close_db(exc):
        db = g.pop("db", None)
        if db is not None:
            db.conn.close()

    @app.route("/api/health")
    def api_health():
        return jsonify({"status": "ok"})

    @app.route("/api/models/status")
    def api_models_status():
        """Lightweight model readiness check for first-launch detection."""
        from models import get_active_model, get_models

        active = get_active_model()
        classification_ready = bool(active and active.get("downloaded"))

        all_models = get_models()
        downloaded_ids = [m["id"] for m in all_models if m.get("downloaded")]

        return jsonify({
            "needs_setup": not classification_ready,
            "classification": {
                "ready": classification_ready,
                "model_name": active["name"] if active else None,
                "model_id": active["id"] if active else None,
            },
            "available_models": [
                {
                    "id": m["id"],
                    "name": m["name"],
                    "description": m.get("description", ""),
                    "size_mb": m.get("size_mb", 0),
                    "downloaded": m.get("downloaded", False),
                    "model_type": m.get("model_type", "bioclip"),
                }
                for m in all_models
            ],
        })

    @app.route("/api/shutdown", methods=["POST"])
    def api_shutdown():
        # Require a non-simple header to block cross-site POSTs.
        # Browsers won't send custom headers cross-origin without a CORS
        # preflight, and we don't serve permissive CORS headers, so a
        # malicious page cannot trigger this endpoint.
        if not request.headers.get("X-Vireo-Shutdown"):
            return json_error("Missing X-Vireo-Shutdown header", 403)

        import signal
        import threading

        def _shutdown():
            os.kill(os.getpid(), signal.SIGTERM)

        threading.Timer(0.5, _shutdown).start()
        return jsonify({"status": "shutting_down"})

    # Load user config (e.g. HF token) on startup
    import config as cfg

    startup_cfg = cfg.load()
    if startup_cfg.get("hf_token"):
        os.environ["HF_TOKEN"] = startup_cfg["hf_token"]

    # Initialize job runner, log broadcaster, and default collections
    _t0 = time.time()
    init_db = Database(db_path)
    log.info("Database init took %.2fs (workspace: %s)", time.time() - _t0,
             init_db.get_workspace(init_db._active_workspace_id)["name"])
    init_db.create_default_collections()

    # Mark species keywords from taxonomy in background (avoids slow startup)
    import threading

    def _mark_species():
        taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
        if not os.path.exists(taxonomy_path):
            return
        try:
            from taxonomy import Taxonomy

            tax = Taxonomy(taxonomy_path)
            bg_db = Database(db_path)
            updated = bg_db.mark_species_keywords(tax)
            if updated:
                log.info("Marked %d keywords as species from taxonomy", updated)
        except Exception:
            log.debug("Could not load taxonomy for species marking", exc_info=True)

    threading.Thread(target=_mark_species, daemon=True).start()

    def _folder_health_loop():
        """Periodically check folder health."""
        import time as _time
        _time.sleep(30)  # Initial delay
        while True:
            try:
                health_db = Database(db_path)
                changed = health_db.check_folder_health()
                if changed:
                    log.info("Folder health check: %d folder(s) changed status", changed)
            except Exception:
                log.debug("Folder health check failed", exc_info=True)
            _time.sleep(600)  # 10 minutes

    threading.Thread(target=_folder_health_loop, daemon=True).start()

    app._job_runner = JobRunner(db=init_db)
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    # -- Page routes --

    @app.route("/")
    def index():
        from models import get_active_model
        active = get_active_model()
        if active and active.get("downloaded"):
            return redirect("/browse")
        user_cfg = cfg.load()
        if user_cfg.get("setup_complete"):
            return redirect("/browse")
        return redirect("/welcome")

    @app.route("/welcome")
    def welcome():
        from models import get_active_model
        active = get_active_model()
        if active and active.get("downloaded") and not request.args.get("force"):
            return redirect("/browse")
        return render_template("welcome.html")

    @app.route("/api/setup/complete", methods=["POST"])
    def api_setup_complete():
        """Mark first-launch setup as done (called after download or skip)."""
        user_cfg = cfg.load()
        user_cfg["setup_complete"] = True
        cfg.save(user_cfg)
        return jsonify({"ok": True})

    @app.route("/browse")
    def browse():
        return render_template("browse.html")

    @app.route("/review")
    def review():
        return render_template("review.html")

    @app.route("/lightroom")
    def lightroom_page():
        return render_template("lightroom.html")

    @app.route("/audit")
    def audit():
        return render_template("audit.html")

    @app.route("/cull")
    def cull_page():
        return render_template("cull.html")

    @app.route("/pipeline")
    def pipeline_page():
        return render_template("pipeline.html")

    @app.route("/pipeline/review")
    def pipeline_review_page():
        return render_template("pipeline_review.html")

    @app.route("/variants")
    def variants_page():
        return render_template("variants.html")

    @app.route("/workspace")
    def workspace_page():
        return render_template("workspace.html")

    @app.route("/compare")
    def compare():
        return render_template("compare.html")

    @app.route("/settings")
    def settings():
        return render_template("settings.html")

    @app.route("/shortcuts")
    def shortcuts_page():
        return render_template("shortcuts.html")

    @app.route("/keywords")
    def keywords_page():
        return render_template("keywords.html")

    @app.route("/jobs")
    def jobs_page():
        return render_template("jobs.html")

    @app.route("/move")
    def move_page():
        return render_template("move.html")

    @app.route("/highlights")
    def highlights_page():
        return render_template("highlights.html")

    # -- API routes --

    def _attach_species(db, photo_dicts):
        """Attach species keyword names to a list of photo dicts (in-place)."""
        if not photo_dicts:
            return photo_dicts
        ids = [p["id"] for p in photo_dicts]
        species_map = db.get_species_keywords_for_photos(ids)
        for p in photo_dicts:
            p["species"] = species_map.get(p["id"], [])
        return photo_dicts

    @app.route("/api/browse/init")
    def api_browse_init():
        """Combined endpoint for browse page initial load — one request instead of five."""
        import config as cfg
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), _MAX_PER_PAGE))
        sort = request.args.get("sort", "date")

        photos = db.get_photos(page=page, per_page=per_page, sort=sort)
        total = db.count_photos()
        folders = db.get_folder_tree()
        keywords = db.get_keyword_tree()
        collections = db.get_collections()

        photo_dicts = [dict(p) for p in photos]
        _attach_species(db, photo_dicts)

        return jsonify(
            {
                "photos": photo_dicts,
                "total": total,
                "page": page,
                "per_page": per_page,
                "folders": [dict(f) for f in folders],
                "keywords": [dict(k) for k in keywords],
                "collections": [dict(c) for c in collections],
            }
        )

    @app.route("/api/pipeline/page-init")
    def api_pipeline_page_init():
        """Combined endpoint for pipeline page initial load."""
        db = _get_db()
        pipeline_counts = db.get_pipeline_feature_counts()
        total_photos = db.count_photos()

        import config as cfg
        from pipeline import load_results
        cache_dir = os.path.dirname(db_path)
        results = load_results(cache_dir, db._active_workspace_id)
        if results and results.get("photos"):
            photo_ids = [p["id"] for p in results["photos"]]
            placeholders = ",".join("?" for _ in photo_ids)
            rows = db.conn.execute(
                f"SELECT id, flag, rating FROM photos WHERE id IN ({placeholders})",
                photo_ids,
            ).fetchall()
            flag_map = {r["id"]: (r["flag"], r["rating"]) for r in rows}
            for p in results["photos"]:
                f, r = flag_map.get(p["id"], ("none", 0))
                p["flag"] = f
                p["rating"] = r
        effective_cfg = db.get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})

        ws = db.get_workspace(db._active_workspace_id)
        ws_overrides = {}
        if ws and ws["config_overrides"]:
            try:
                ws_overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except Exception:
                pass

        taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
        taxonomy_available = os.path.exists(taxonomy_path)

        return jsonify({
            "total_photos": total_photos,
            "has_detections": pipeline_counts["detections"],
            "has_masks": pipeline_counts["masks"],
            "has_sharpness": pipeline_counts["sharpness"],
            "taxonomy_available": taxonomy_available,
            "pipeline_config": {
                "sam2_variant": pipeline_cfg.get("sam2_variant", "sam2-small"),
                "dinov2_variant": pipeline_cfg.get("dinov2_variant", "vit-b14"),
                "proxy_longest_edge": pipeline_cfg.get("proxy_longest_edge", 1536),
            },
            "results": results,
            "workspace_overrides": ws_overrides,
        })

    @app.route("/api/folders")
    def api_folders():
        db = _get_db()
        folders = db.get_folder_tree()
        return jsonify([dict(f) for f in folders])

    @app.route("/api/folders/missing")
    def api_folders_missing():
        db = _get_db()
        missing = db.get_missing_folders()
        return jsonify([dict(f) for f in missing])

    @app.route("/api/folders/<int:folder_id>/relocate", methods=["POST"])
    def api_folder_relocate(folder_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        new_path = body.get("path", "")
        if not new_path:
            return json_error("path is required")
        if not os.path.isdir(new_path):
            return json_error("path does not exist or is not a directory")

        try:
            cascaded = db.relocate_folder(folder_id, new_path)
        except ValueError as e:
            return json_error(str(e), 409)
        return jsonify({
            "status": "ok",
            "cascaded": cascaded,
        })

    @app.route("/api/folders/<int:folder_id>", methods=["DELETE"])
    def api_folder_delete(folder_id):
        db = _get_db()
        result = db.delete_folder(folder_id)
        return jsonify(result)

    @app.route("/api/photos")
    def api_photos():
        import config as cfg
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), _MAX_PER_PAGE))
        sort = request.args.get("sort", "date")
        folder_id = request.args.get("folder_id", None, type=int)
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)
        color_label = request.args.get("color_label", None)

        photos = db.get_photos(
            folder_id=folder_id,
            page=page,
            per_page=per_page,
            sort=sort,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
            color_label=color_label,
        )

        # Total count — use count_photos for unfiltered, otherwise use efficient COUNT query
        if not any([folder_id, rating_min, date_from, date_to, keyword, color_label]):
            total = db.count_photos()
        else:
            total = db.count_filtered_photos(
                folder_id=folder_id,
                rating_min=rating_min,
                date_from=date_from,
                date_to=date_to,
                keyword=keyword,
                color_label=color_label,
            )

        photo_dicts = [dict(p) for p in photos]
        _attach_species(db, photo_dicts)

        return jsonify(
            {
                "photos": photo_dicts,
                "total": total,
                "page": page,
                "per_page": per_page,
            }
        )

    @app.route("/api/photos/calendar")
    def api_photos_calendar():
        db = _get_db()
        from datetime import date

        year = request.args.get("year", date.today().year, type=int)
        folder_id = request.args.get("folder_id", None, type=int)
        rating_min = request.args.get("rating_min", None, type=int)
        keyword = request.args.get("keyword", None)
        color_label = request.args.get("color_label", None)
        data = db.get_calendar_data(
            year=year, folder_id=folder_id, rating_min=rating_min, keyword=keyword,
            color_label=color_label,
        )
        return jsonify(data)

    @app.route("/api/browse/summary")
    def api_browse_summary():
        db = _get_db()
        folder_id = request.args.get("folder_id", None, type=int)
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)
        collection_id = request.args.get("collection_id", None, type=int)
        color_label = request.args.get("color_label", None)
        return jsonify(
            db.get_browse_summary(
                folder_id=folder_id,
                rating_min=rating_min,
                date_from=date_from,
                date_to=date_to,
                keyword=keyword,
                collection_id=collection_id,
                color_label=color_label,
            )
        )

    @app.route("/api/photos/color_labels")
    def api_photos_color_labels():
        db = _get_db()
        ids_str = request.args.get("ids", "")
        if not ids_str:
            return jsonify({})
        photo_ids = [int(x) for x in ids_str.split(",") if x.strip().isdigit()]
        labels = db.get_color_labels_for_photos(photo_ids)
        return jsonify(labels)

    @app.route("/api/photos/<int:photo_id>")
    def api_photo_detail(photo_id):
        db = _get_db()
        photo = db.get_photo(photo_id)
        if not photo:
            return json_error("not found", 404)

        result = dict(photo)

        # Parse exif_data JSON into metadata field
        raw_exif = result.pop("exif_data", None)
        if raw_exif:
            try:
                result["metadata"] = json.loads(raw_exif)
            except (ValueError, TypeError):
                result["metadata"] = None
        else:
            result["metadata"] = None

        keywords = db.get_photo_keywords(photo_id)
        result["keywords"] = [dict(k) for k in keywords]

        # Read XMP sidecar keywords
        folder = db.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
        ).fetchone()
        if folder:
            xmp_path = os.path.join(
                folder["path"],
                os.path.splitext(photo["filename"])[0] + ".xmp",
            )
            xmp_keywords = []
            xmp_exists = os.path.exists(xmp_path)
            if xmp_exists:
                from xmp import read_keywords

                xmp_keywords = sorted(read_keywords(xmp_path))
            result["xmp_exists"] = xmp_exists
            result["xmp_keywords"] = xmp_keywords
            result["xmp_path"] = xmp_path
        else:
            result["xmp_exists"] = False
            result["xmp_keywords"] = []
            result["xmp_path"] = ""

        return jsonify(result)

    @app.route("/api/photos/geo")
    def api_photos_geo():
        db = _get_db()
        folder_id = request.args.get("folder_id", None, type=int)
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)
        species = request.args.get("species", None)

        photos = db.get_geolocated_photos(
            folder_id=folder_id,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
            species=species,
        )

        total_photos = db.count_photos()
        total_without_gps = db.count_photos_without_gps()
        total_with_gps = total_photos - total_without_gps

        return jsonify({
            "photos": [dict(p) for p in photos],
            "total_filtered": len(photos),
            "total_photos": total_photos,
            "total_with_gps": total_with_gps,
            "total_without_gps": total_without_gps,
        })

    @app.route("/api/species")
    def api_species():
        db = _get_db()
        species = db.get_accepted_species()
        return jsonify({"species": species})

    @app.route("/api/keywords/all")
    def api_all_keywords():
        db = _get_db()
        keywords = db.get_all_keywords()
        return jsonify([dict(k) for k in keywords])

    @app.route("/api/keywords")
    def api_keywords():
        db = _get_db()
        keywords = db.get_keyword_tree()
        return jsonify([dict(k) for k in keywords])

    @app.route("/api/keywords/duplicates")
    def api_keyword_duplicates():
        """Find case-insensitive duplicate keywords within current workspace."""
        db = _get_db()
        ws = db._active_workspace_id
        dupes = db.conn.execute(
            """SELECT LOWER(k.name) as lname, GROUP_CONCAT(k.id) as ids,
                      GROUP_CONCAT(k.name, ' | ') as names, COUNT(DISTINCT k.id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
               GROUP BY LOWER(k.name) HAVING COUNT(DISTINCT k.id) > 1""",
            (ws,),
        ).fetchall()
        results = []
        for d in dupes:
            ids = list(set(int(x) for x in d["ids"].split(",")))
            # Count photos per variant within this workspace
            variants = []
            for kid in ids:
                row = db.conn.execute(
                    """SELECT k.name, COUNT(pk.photo_id) as cnt
                       FROM keywords k
                       JOIN photo_keywords pk ON pk.keyword_id = k.id
                       JOIN photos p ON p.id = pk.photo_id
                       JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                       WHERE k.id = ? AND wf.workspace_id = ?""",
                    (kid, ws),
                ).fetchone()
                if row and row["cnt"] > 0:
                    variants.append({"id": kid, "name": row["name"], "photo_count": row["cnt"]})
            if len(variants) > 1:
                results.append({"variants": variants, "keep": variants[0]["name"]})
        return jsonify(results)

    @app.route("/api/keywords/clean", methods=["POST"])
    def api_clean_keywords():
        """Merge case-insensitive duplicate keywords."""
        db = _get_db()
        merged = db.merge_duplicate_keywords()
        log.info("Keyword cleanup: merged %d duplicates", merged)
        return jsonify({"ok": True, "merged": merged})

    def _queue_keyword_add(photo_id, keyword_name, workspace_id=None):
        """Queue a keyword add unless it cancels a pending removal."""
        db = _get_db()
        removed = db.remove_pending_changes(photo_id, "keyword_remove", keyword_name, workspace_id=workspace_id)
        if removed == 0:
            db.queue_change(photo_id, "keyword_add", keyword_name, workspace_id=workspace_id)

    def _queue_keyword_remove(photo_id, keyword_name, workspace_id=None):
        """Queue a keyword removal unless it cancels a pending add."""
        db = _get_db()
        removed = db.remove_pending_changes(photo_id, "keyword_add", keyword_name, workspace_id=workspace_id)
        if removed == 0:
            db.queue_change(photo_id, "keyword_remove", keyword_name, workspace_id=workspace_id)

    # -- Edit API routes --

    @app.route("/api/photos/<int:photo_id>/rating", methods=["POST"])
    def api_set_rating(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        rating = body.get("rating", 0)
        if isinstance(rating, bool) or not isinstance(rating, int) or rating < 0 or rating > 5:
            return json_error("rating must be an integer 0-5")
        old = db.get_photo(photo_id)
        old_rating = old["rating"] if old else 0
        db.update_photo_rating(photo_id, rating)
        db.queue_change(photo_id, "rating", str(rating))
        db.record_edit('rating', f'Set rating to {rating}', str(rating),
                       [{'photo_id': photo_id, 'old_value': str(old_rating), 'new_value': str(rating)}])
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/flag", methods=["POST"])
    def api_set_flag(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        flag = body.get("flag", "none")
        if flag not in ("none", "flagged", "rejected"):
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        old = db.get_photo(photo_id)
        old_flag = old["flag"] if old else "none"
        db.update_photo_flag(photo_id, flag)
        db.record_edit('flag', f'Set flag to {flag}', flag,
                       [{'photo_id': photo_id, 'old_value': old_flag, 'new_value': flag}])
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/color_label", methods=["POST"])
    def api_set_color_label(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        color = body.get("color")
        if color is not None and color not in db.VALID_COLOR_LABELS:
            return json_error(f"color must be one of {db.VALID_COLOR_LABELS}")
        old_color = db.get_color_label(photo_id) or ''
        new_color = color or ''
        if color:
            db.set_color_label(photo_id, color)
        else:
            db.remove_color_label(photo_id)
        db.record_edit('color_label', f'Set color to {color or "none"}', new_color,
                       [{'photo_id': photo_id, 'old_value': old_color, 'new_value': new_color}])
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/keywords", methods=["POST"])
    def api_add_keyword(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return json_error("name required")
        kid = db.add_keyword(name)
        # Override type if explicitly provided
        kw_type = body.get("type")
        if kw_type and kw_type in ('general', 'taxonomy', 'location', 'descriptive', 'people', 'event'):
            db.conn.execute("UPDATE keywords SET type = ? WHERE id = ?", (kw_type, kid))
            db.conn.commit()
        db.tag_photo(photo_id, kid)
        _queue_keyword_add(photo_id, name)
        db.record_edit('keyword_add', f'Added keyword "{name}"', str(kid),
                       [{'photo_id': photo_id, 'old_value': '', 'new_value': str(kid)}])
        return jsonify({"ok": True, "keyword_id": kid})

    @app.route(
        "/api/photos/<int:photo_id>/keywords/<int:keyword_id>", methods=["DELETE"]
    )
    def api_remove_keyword(photo_id, keyword_id):
        db = _get_db()
        keywords = db.get_photo_keywords(photo_id)
        kw_name = ""
        for k in keywords:
            if k["id"] == keyword_id:
                kw_name = k["name"]
                break
        db.untag_photo(photo_id, keyword_id)
        _queue_keyword_remove(photo_id, kw_name)
        db.record_edit('keyword_remove', f'Removed keyword "{kw_name}"', str(keyword_id),
                       [{'photo_id': photo_id, 'old_value': str(keyword_id), 'new_value': ''}])
        return jsonify({"ok": True})

    @app.route("/api/keywords/<int:keyword_id>", methods=["PUT"])
    def api_update_keyword(keyword_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        # Capture old name before update for sidecar queuing
        new_name = body.get("name")
        old_name = None
        if new_name:
            old_row = db.conn.execute(
                "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
            ).fetchone()
            if old_row and old_row["name"] != new_name:
                old_name = old_row["name"]
        # Apply the update first — if it raises, no sidecar changes are queued
        try:
            db.update_keyword(keyword_id, **body)
        except ValueError as e:
            return json_error(str(e), 400)
        # Queue sidecar updates only after successful DB update, for all affected workspaces
        if old_name:
            affected = db.conn.execute(
                """SELECT pk.photo_id, wf.workspace_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE pk.keyword_id = ?""",
                (keyword_id,),
            ).fetchall()
            for row in affected:
                _queue_keyword_remove(row["photo_id"], old_name, workspace_id=row["workspace_id"])
                _queue_keyword_add(row["photo_id"], new_name, workspace_id=row["workspace_id"])
        return jsonify({"ok": True})

    @app.route("/api/keywords/<int:keyword_id>", methods=["DELETE"])
    def api_delete_keyword(keyword_id):
        db = _get_db()
        # Queue sidecar removals for all affected workspaces
        kw_row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if kw_row:
            affected = db.conn.execute(
                """SELECT pk.photo_id, wf.workspace_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE pk.keyword_id = ?""",
                (keyword_id,),
            ).fetchall()
            for row in affected:
                _queue_keyword_remove(row["photo_id"], kw_row["name"], workspace_id=row["workspace_id"])
        db.conn.execute("UPDATE keywords SET parent_id = NULL WHERE parent_id = ?", (keyword_id,))
        db.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (keyword_id,))
        db.conn.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        db.conn.commit()
        return jsonify({"ok": True})

    # -- Batch operations --

    @app.route("/api/batch/rating", methods=["POST"])
    def api_batch_rating():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        rating = body.get("rating", 0)
        if isinstance(rating, bool) or not isinstance(rating, int) or rating < 0 or rating > 5:
            return json_error("rating must be an integer 0-5")
        if not photo_ids:
            return json_error("photo_ids required")
        photos_map = db.get_photos_by_ids(photo_ids)
        old_values = {pid: photos_map[pid]["rating"] for pid in photo_ids if pid in photos_map}
        valid_ids = list(old_values.keys())
        db.batch_update_photo_rating(valid_ids, rating)
        for pid in valid_ids:
            db.queue_change(pid, "rating", str(rating))
        items = [{'photo_id': pid, 'old_value': str(old_values[pid]), 'new_value': str(rating)} for pid in old_values]
        db.record_edit('rating', f'Set rating to {rating} on {len(photo_ids)} photos',
                       str(rating), items, is_batch=True)
        return jsonify({"ok": True, "updated": len(old_values)})

    @app.route("/api/batch/flag", methods=["POST"])
    def api_batch_flag():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        flag = body.get("flag", "none")
        if flag not in ("none", "flagged", "rejected"):
            return json_error("flag must be 'none', 'flagged', or 'rejected'")
        if not photo_ids:
            return json_error("photo_ids required")
        photos_map = db.get_photos_by_ids(photo_ids)
        old_values = {pid: photos_map[pid]["flag"] for pid in photo_ids if pid in photos_map}
        valid_ids = list(old_values.keys())
        db.batch_update_photo_flag(valid_ids, flag)
        items = [{'photo_id': pid, 'old_value': old_values[pid], 'new_value': flag} for pid in old_values]
        db.record_edit('flag', f'Set flag to {flag} on {len(photo_ids)} photos',
                       flag, items, is_batch=True)
        return jsonify({"ok": True, "updated": len(old_values)})

    @app.route("/api/batch/color_label", methods=["POST"])
    def api_batch_color_label():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        color = body.get("color")
        if color is not None and color not in db.VALID_COLOR_LABELS:
            return json_error(f"color must be one of {db.VALID_COLOR_LABELS}")
        if not photo_ids:
            return json_error("photo_ids required")
        old_labels = db.get_color_labels_for_photos(photo_ids)
        new_color = color or ''
        db.batch_set_color_label(photo_ids, color)
        items = [{'photo_id': pid, 'old_value': old_labels.get(pid, ''), 'new_value': new_color}
                 for pid in photo_ids]
        db.record_edit('color_label', f'Set color to {color or "none"} on {len(photo_ids)} photos',
                       new_color, items, is_batch=True)
        return jsonify({"ok": True, "updated": len(photo_ids)})

    @app.route("/api/batch/keyword", methods=["POST"])
    def api_batch_keyword():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        name = body.get("name", "").strip()
        if not photo_ids or not name:
            return json_error("photo_ids and name required")
        kid = db.add_keyword(name)
        kw_type = body.get("type")
        if kw_type and kw_type in ('general', 'taxonomy', 'location', 'descriptive', 'people', 'event'):
            db.conn.execute("UPDATE keywords SET type = ? WHERE id = ?", (kw_type, kid))
            db.conn.commit()
        for pid in photo_ids:
            db.tag_photo(pid, kid)
            _queue_keyword_add(pid, name)
        items = [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)} for pid in photo_ids]
        db.record_edit('keyword_add', f'Added "{name}" to {len(photo_ids)} photos',
                       str(kid), items, is_batch=True)
        return jsonify({"ok": True, "updated": len(photo_ids)})

    @app.route("/api/batch/delete", methods=["POST"])
    def api_batch_delete():
        """Delete photos from Vireo, optionally moving files to trash."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        mode = body.get("mode", "vireo")
        include_companions = body.get("include_companions", False)
        # For disk_permanent retry: accept paths directly since DB rows
        # were already deleted in the initial disk-mode call.
        paths = body.get("paths", [])

        if mode == "disk_permanent" and paths:
            # Retry path: DB already cleaned up, just delete files
            trashed = 0
            trash_failed = []
            for p in paths:
                if not os.path.isfile(p):
                    continue
                try:
                    os.remove(p)
                    trashed += 1
                except OSError:
                    log.warning("Permanent delete failed for %s", p, exc_info=True)
                    trash_failed.append({"path": p})
            return jsonify({
                "ok": True, "deleted": 0, "trashed": trashed,
                "trash_failed": trash_failed,
            })

        if not photo_ids:
            return json_error("photo_ids required")
        if mode not in ("vireo", "disk", "disk_permanent"):
            return json_error("mode must be 'vireo', 'disk', or 'disk_permanent'")

        result = db.delete_photos(photo_ids, include_companions=include_companions)

        # Clean up cached files (thumbnails, previews)
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        preview_dir = os.path.join(os.path.dirname(thumb_dir), "previews")
        for f in result["files"]:
            pid = f["photo_id"]
            for d in [thumb_dir, preview_dir]:
                cached = os.path.join(d, f"{pid}.jpg")
                if os.path.isfile(cached):
                    os.remove(cached)

        trashed = 0
        trash_failed = []
        if mode in ("disk", "disk_permanent"):
            for f in result["files"]:
                # Collect all files to delete: primary + companion
                file_paths = []
                primary = os.path.join(f["folder_path"], f["filename"])
                file_paths.append(primary)
                if include_companions and f.get("companion_path"):
                    companion = os.path.join(f["folder_path"], f["companion_path"])
                    file_paths.append(companion)

                for filepath in file_paths:
                    if not os.path.isfile(filepath):
                        log.warning("File already missing: %s", filepath)
                        continue
                    if mode == "disk":
                        try:
                            from send2trash import send2trash as _trash
                            _trash(filepath)
                            trashed += 1
                        except Exception:
                            log.debug("send2trash failed for %s, trying Finder", filepath)
                            try:
                                _trash_via_finder(filepath)
                                trashed += 1
                            except Exception:
                                log.warning("Trash failed for %s", filepath, exc_info=True)
                                trash_failed.append({"path": filepath})
                    else:  # disk_permanent
                        try:
                            os.remove(filepath)
                            trashed += 1
                        except OSError:
                            log.warning("Permanent delete failed for %s", filepath, exc_info=True)
                            trash_failed.append({"path": filepath})

        return jsonify({
            "ok": True,
            "deleted": result["deleted"],
            "trashed": trashed,
            "trash_failed": trash_failed,
        })

    # -- Undo --

    @app.route("/api/undo", methods=["POST"])
    def api_undo():
        db = _get_db()
        result = db.undo_last_edit()
        if result is None:
            return json_error("nothing to undo")
        return jsonify({"ok": True, "undone": result["description"]})

    @app.route("/api/undo/status")
    def api_undo_status():
        db = _get_db()
        from db import Database
        non_undoable = Database._NON_UNDOABLE
        placeholders = ",".join("?" for _ in non_undoable)
        latest = db.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (db._ws_id(), *non_undoable),
        ).fetchone()
        if not latest:
            return jsonify({"available": False, "description": "", "count": 0})
        total = db.conn.execute(
            f"SELECT COUNT(*) FROM edit_history WHERE workspace_id = ? AND undone = 0 AND action_type NOT IN ({placeholders})",
            (db._ws_id(), *non_undoable),
        ).fetchone()[0]
        return jsonify({
            "available": True,
            "description": latest["description"],
            "count": total,
        })

    @app.route("/api/redo", methods=["POST"])
    def api_redo():
        db = _get_db()
        result = db.redo_last_undo()
        if result is None:
            return json_error("nothing to redo")
        return jsonify({"ok": True, "redone": result["description"]})

    @app.route("/api/redo/status")
    def api_redo_status():
        db = _get_db()
        from db import Database
        non_undoable = Database._NON_UNDOABLE
        placeholders = ",".join("?" for _ in non_undoable)
        latest = db.conn.execute(
            f"SELECT * FROM edit_history WHERE workspace_id = ? AND undone = 1 AND action_type NOT IN ({placeholders}) "
            "ORDER BY created_at ASC, id ASC LIMIT 1",
            (db._ws_id(), *non_undoable),
        ).fetchone()
        if not latest:
            return jsonify({"available": False, "description": ""})
        return jsonify({
            "available": True,
            "description": latest["description"],
        })

    @app.route("/api/edit-history")
    def api_edit_history():
        db = _get_db()
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        return jsonify(db.get_edit_history(limit=limit, offset=offset))

    # -- Statistics --

    @app.route("/api/stats")
    def api_stats():
        db = _get_db()
        stats = db.get_dashboard_stats()
        stats["total_photos"] = db.count_photos()
        return jsonify(stats)

    @app.route("/api/sync/status")
    def api_sync_status():
        db = _get_db()
        changes = db.get_pending_changes()
        return jsonify(
            {
                "pending_count": len(changes),
            }
        )

    @app.route("/api/sync/preview")
    def api_sync_preview():
        """Preview all pending changes grouped by photo."""
        db = _get_db()
        changes = db.get_pending_changes()
        if not changes:
            return jsonify({"photos": [], "total_changes": 0})

        # Group by photo
        by_photo = {}
        for c in changes:
            pid = c["photo_id"]
            if pid not in by_photo:
                photo = db.get_photo(pid)
                folder = db.conn.execute(
                    "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
                ).fetchone()
                by_photo[pid] = {
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "folder": folder["path"] if folder else "",
                    "changes": [],
                }
            by_photo[pid]["changes"].append({
                "id": c["id"],
                "type": c["change_type"],
                "value": c["value"],
            })

        return jsonify({
            "photos": list(by_photo.values()),
            "total_changes": len(changes),
        })

    @app.route("/api/sync/discard", methods=["POST"])
    def api_sync_discard():
        """Discard specific pending changes."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        change_ids = body.get("change_ids", [])
        if not change_ids:
            return json_error("change_ids required")

        # Look up changes before deleting so we can record what was discarded
        placeholders = ",".join("?" for _ in change_ids)
        changes = db.conn.execute(
            f"SELECT * FROM pending_changes WHERE id IN ({placeholders}) AND workspace_id = ?",
            list(change_ids) + [db._ws_id()],
        ).fetchall()

        db.clear_pending(change_ids)

        # Record discard in history (not undoable)
        if changes:
            items = [{'photo_id': c['photo_id'],
                      'old_value': f'{c["change_type"]}:{c["value"]}',
                      'new_value': ''}
                     for c in changes]
            db.record_edit('discard',
                           f'Discarded {len(changes)} pending changes',
                           '', items, is_batch=len(changes) > 1)

        log.info("Discarded %d pending changes", len(change_ids))
        return jsonify({"ok": True, "discarded": len(change_ids)})

    # -- Collection API routes --

    @app.route("/api/collections")
    def api_collections():
        db = _get_db()
        collections = db.get_collections()
        result = []
        for c in collections:
            d = dict(c)
            d["photo_count"] = db.count_collection_photos(c["id"])
            result.append(d)
        return jsonify(result)

    @app.route("/api/collections", methods=["POST"])
    def api_create_collection():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        import json

        name = body.get("name", "").strip()
        rules = body.get("rules", [])
        if not name:
            return json_error("name required")
        cid = db.add_collection(name, json.dumps(rules))
        return jsonify({"ok": True, "id": cid})

    @app.route("/api/collections/<int:collection_id>", methods=["DELETE"])
    def api_delete_collection(collection_id):
        db = _get_db()
        db.delete_collection(collection_id)
        return jsonify({"ok": True})

    @app.route("/api/collections/<int:collection_id>/add-photos", methods=["POST"])
    def api_collection_add_photos(collection_id):
        """Add photos to a static collection by appending to its photo_ids rule."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        if not photo_ids:
            return json_error("photo_ids required")

        row = db.conn.execute(
            "SELECT rules FROM collections WHERE id = ?", (collection_id,)
        ).fetchone()
        if not row:
            return json_error("Collection not found", 404)

        rules = json.loads(row["rules"])
        # Find or create a photo_ids rule
        ids_rule = None
        for r in rules:
            if r.get("field") == "photo_ids":
                ids_rule = r
                break
        if ids_rule is None:
            ids_rule = {"field": "photo_ids", "value": []}
            rules.append(ids_rule)

        # Merge new IDs
        existing = set(ids_rule["value"])
        for pid in photo_ids:
            existing.add(pid)
        ids_rule["value"] = sorted(existing)

        db.conn.execute(
            "UPDATE collections SET rules = ? WHERE id = ?",
            (json.dumps(rules), collection_id),
        )
        db.conn.commit()
        return jsonify({"ok": True, "total": len(ids_rule["value"])})

    @app.route("/api/collections/<int:collection_id>/photos")
    def api_collection_photos(collection_id):
        import config as cfg
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), _MAX_PER_PAGE))
        photos = db.get_collection_photos(collection_id, page=page, per_page=per_page)
        total = db.count_collection_photos(collection_id)
        return jsonify(
            {
                "photos": [dict(p) for p in photos],
                "page": page,
                "per_page": per_page,
                "total": total,
            }
        )

    # -- Highlights --

    @app.route("/api/highlights")
    def api_highlights():
        db = _get_db()

        folders = db.get_folders_with_quality_data()
        if not folders:
            return jsonify({
                "photos": [],
                "meta": {"total_in_folder": 0, "eligible": 0, "species_breakdown": {}},
                "folders": [],
            })

        folder_id = request.args.get("folder_id", type=int)
        if folder_id is None:
            folder_id = folders[0]["id"]  # Most recent

        count = request.args.get("count", type=int)
        max_per_species = request.args.get("max_per_species", 5, type=int)
        min_quality = request.args.get("min_quality", 0.0, type=float)

        candidates = db.get_highlights_candidates(folder_id, min_quality=min_quality)
        total_in_folder = db.count_filtered_photos(folder_id=folder_id)

        # Adaptive default: 5% clamped to [10, 50]
        if count is None:
            count = max(10, min(50, int(len(candidates) * 0.05))) if candidates else 0

        selected = select_highlights(
            [dict(r) for r in candidates],
            count=count,
            max_per_species=max_per_species,
        )

        # Build species breakdown
        species_counts = {}
        for p in selected:
            sp = p.get("species") or "Unidentified"
            species_counts[sp] = species_counts.get(sp, 0) + 1

        # Strip binary fields before JSON response
        photo_list = []
        for p in selected:
            out = {k: v for k, v in p.items()
                   if k not in ("dino_subject_embedding", "dino_global_embedding")}
            photo_list.append(out)

        return jsonify({
            "photos": photo_list,
            "meta": {
                "total_in_folder": total_in_folder,
                "eligible": len(candidates),
                "species_breakdown": species_counts,
                "avg_quality": round(sum(p.get("quality_score", 0) for p in selected) / max(len(selected), 1), 2),
            },
            "folders": [{"id": f["id"], "name": f["name"], "photo_count": f["photo_count"]} for f in folders],
        })

    @app.route("/api/highlights/save", methods=["POST"])
    def api_highlights_save():
        db = _get_db()

        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        name = body.get("name", "").strip()

        if not photo_ids:
            return json_error("photo_ids required")
        if not name:
            return json_error("name required")

        rules = json.dumps([{"field": "photo_ids", "value": photo_ids}])
        cid = db.add_collection(name, rules)
        return jsonify({"ok": True, "id": cid})

    # -- Workspace API routes --

    @app.route("/api/workspaces")
    def api_get_workspaces():
        db = _get_db()
        workspaces = db.get_workspaces()
        return jsonify([dict(w) for w in workspaces])

    @app.route("/api/workspaces/active")
    def api_get_active_workspace():
        db = _get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if not ws:
            return json_error("No active workspace", 404)
        result = dict(ws)
        result["folders"] = [dict(f) for f in db.get_workspace_folders(ws["id"])]
        return jsonify(result)

    @app.route("/api/workspaces", methods=["POST"])
    def api_create_workspace():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return json_error("Name is required")
        try:
            ws_id = db.create_workspace(name, config_overrides=body.get("config_overrides"))
            # Link selected folders if provided
            for folder_id in body.get("folder_ids", []):
                db.add_workspace_folder(ws_id, folder_id)
            ws = db.get_workspace(ws_id)
            return jsonify(dict(ws))
        except Exception as e:
            return json_error(str(e))

    @app.route("/api/workspaces/<int:ws_id>", methods=["PUT"])
    def api_update_workspace(ws_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        kwargs = {}
        if "name" in body:
            kwargs["name"] = body["name"]
        if "config_overrides" in body:
            kwargs["config_overrides"] = body["config_overrides"]
        if "ui_state" in body:
            kwargs["ui_state"] = body["ui_state"]
        db.update_workspace(ws_id, **kwargs)
        ws = db.get_workspace(ws_id)
        return jsonify(dict(ws))

    @app.route("/api/workspaces/<int:ws_id>", methods=["DELETE"])
    def api_delete_workspace(ws_id):
        db = _get_db()
        # Prevent deleting the last workspace
        workspaces = db.get_workspaces()
        if len(workspaces) <= 1:
            return json_error("Cannot delete the only workspace")
        # Prevent deleting the active workspace
        if ws_id == db._active_workspace_id:
            return json_error("Cannot delete the active workspace. Switch first.")
        db.delete_workspace(ws_id)
        return jsonify({"ok": True})

    @app.route("/api/workspaces/<int:ws_id>/activate", methods=["POST"])
    def api_activate_workspace(ws_id):
        db = _get_db()
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

    @app.route("/api/workspaces/<int:ws_id>/folders", methods=["GET"])
    def api_workspace_folders(ws_id):
        db = _get_db()
        folders = db.get_workspace_folders(ws_id)
        return jsonify([dict(f) for f in folders])

    @app.route("/api/workspaces/<int:ws_id>/folders", methods=["POST"])
    def api_add_workspace_folder(ws_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        folder_id = body.get("folder_id")
        if not folder_id:
            return json_error("folder_id is required")
        db.add_workspace_folder(ws_id, folder_id)
        return jsonify({"ok": True})

    @app.route("/api/workspaces/<int:ws_id>/folders/<int:folder_id>", methods=["DELETE"])
    def api_remove_workspace_folder(ws_id, folder_id):
        db = _get_db()
        db.remove_workspace_folder(ws_id, folder_id)
        return jsonify({"ok": True})

    @app.route("/api/workspaces/<int:ws_id>/move-folders", methods=["POST"])
    def api_move_workspace_folders(ws_id):
        db = _get_db()
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

        # Validate source workspace and folder ownership before creating a
        # new workspace to avoid orphans if the move would fail.
        if new_ws_name:
            if not db.get_workspace(ws_id):
                return json_error(f"Source workspace {ws_id} not found")
            source_folder_ids = {f["id"] for f in db.get_workspace_folders(ws_id)}
            for fid in folder_ids:
                if fid not in source_folder_ids:
                    return json_error(f"Folder {fid} does not belong to source workspace {ws_id}")
            try:
                target_ws_id = db.create_workspace(new_ws_name)
            except Exception as e:
                return json_error(f"Failed to create workspace: {e}")

        try:
            result = db.move_folders_to_workspace(ws_id, target_ws_id, folder_ids)
            result["target_workspace_id"] = target_ws_id
            return jsonify(result)
        except ValueError as e:
            return json_error(str(e))

    @app.route("/api/workspaces/active/config")
    def api_workspace_config():
        """Get the active workspace's config overrides."""
        db = _get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if not ws:
            return jsonify({})
        overrides = {}
        if ws["config_overrides"]:
            try:
                overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except Exception:
                pass
        return jsonify(overrides)

    @app.route("/api/workspaces/active/config", methods=["POST"])
    def api_set_workspace_config():
        """Set config overrides for the active workspace."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        # Only allow workspace-overridable keys
        allowed = {"classification_threshold", "grouping_window_seconds", "similarity_threshold", "review_min_confidence"}
        # Merge into existing overrides to preserve non-whitelisted keys
        ws = db.get_workspace(db._active_workspace_id)
        existing = {}
        if ws and ws["config_overrides"]:
            try:
                existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except Exception:
                pass
        for k, v in body.items():
            if k in allowed:
                if v is None:
                    existing.pop(k, None)
                else:
                    existing[k] = v
        db.update_workspace(db._active_workspace_id, config_overrides=existing if existing else None)
        return jsonify({"ok": True, "overrides": existing})

    @app.route("/api/workspaces/active/nav-order", methods=["PUT"])
    def api_set_nav_order():
        """Save navbar link order for the active workspace."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        nav_order = body.get("nav_order")
        if not isinstance(nav_order, list):
            return jsonify({"error": "nav_order must be a list"}), 400
        ws = db.get_workspace(db._active_workspace_id)
        existing = {}
        if ws and ws["config_overrides"]:
            try:
                existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except Exception:
                pass
        existing["nav_order"] = nav_order
        db.update_workspace(db._active_workspace_id, config_overrides=existing)
        return jsonify({"ok": True, "nav_order": nav_order})

    # -- Prediction API routes --

    @app.route("/api/predictions")
    def api_predictions():
        db = _get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        status = request.args.get("status", None)
        if collection_id:
            photos = db.get_collection_photos(collection_id, per_page=999999)
            photo_ids = [p["id"] for p in photos]
            preds = (
                db.get_predictions(photo_ids=photo_ids, status=status)
                if photo_ids
                else []
            )
        else:
            preds = db.get_predictions(status=status)

        # Fetch alternatives to attach to their parent predictions
        alt_preds = []
        if not status or status == "pending":
            if collection_id:
                if photo_ids:
                    alt_preds = db.get_predictions(photo_ids=photo_ids, status="alternative")
            else:
                alt_preds = db.get_predictions(status="alternative")

        # Index alternatives by (detection_id, model)
        alts_by_key = {}
        for a in alt_preds:
            ad = dict(a)
            key = (ad["detection_id"], ad["model"])
            alts_by_key.setdefault(key, []).append({
                "id": ad["id"],
                "species": ad["species"],
                "confidence": ad["confidence"],
                "taxonomy_kingdom": ad.get("taxonomy_kingdom"),
                "taxonomy_phylum": ad.get("taxonomy_phylum"),
                "taxonomy_class": ad.get("taxonomy_class"),
                "taxonomy_order": ad.get("taxonomy_order"),
                "taxonomy_family": ad.get("taxonomy_family"),
                "taxonomy_genus": ad.get("taxonomy_genus"),
                "scientific_name": ad.get("scientific_name"),
            })

        # Enrich predictions and attach alternatives
        results = []
        for p in preds:
            d = dict(p)
            if d.get("status") == "alternative":
                continue  # alternatives are nested, not top-level
            if d.get("category") in ("disagreement", "refinement"):
                keywords = db.get_photo_keywords(d["photo_id"])
                existing_species = [
                    k["name"] for k in keywords
                    if db.is_keyword_species(k["id"])
                ]
                d["existing_species"] = existing_species
            # Attach alternatives
            key = (d.get("detection_id"), d.get("model"))
            d["alternatives"] = alts_by_key.get(key, [])
            results.append(d)
        return jsonify(results)

    @app.route("/api/predictions/compare")
    def api_predictions_compare():
        db = _get_db()
        collection_id = request.args.get("collection_id", None, type=int)
        if not collection_id:
            return jsonify({"error": "collection_id required"}), 400

        photos = db.get_collection_photos(collection_id, per_page=999999)
        photo_ids = [p["id"] for p in photos]
        if not photo_ids:
            return jsonify({"models": [], "photos": []})

        preds = db.get_predictions(photo_ids=photo_ids)

        # Collect distinct models and build per-photo lookup
        # With multi-detection, each photo may have multiple predictions per model
        models = set()
        by_photo = {}
        for pr in preds:
            d = dict(pr)
            pid = d["photo_id"]
            model = d["model"]
            models.add(model)
            if pid not in by_photo:
                by_photo[pid] = {"photo_id": pid, "filename": d["filename"], "predictions": {}}
            if model not in by_photo[pid]["predictions"]:
                by_photo[pid]["predictions"][model] = []
            by_photo[pid]["predictions"][model].append({
                "species": d["species"],
                "confidence": d["confidence"],
                "box_x": d.get("box_x"),
                "box_y": d.get("box_y"),
                "box_w": d.get("box_w"),
                "box_h": d.get("box_h"),
            })

        return jsonify({
            "models": sorted(models),
            "photos": list(by_photo.values()),
        })

    @app.route("/api/predictions/<int:pred_id>/accept", methods=["POST"])
    def api_accept_prediction(pred_id):
        db = _get_db()
        result = db.accept_prediction(pred_id)
        if result:
            items = [{'photo_id': a['photo_id'],
                      'old_value': str(a['prediction_id']),
                      'new_value': str(result['keyword_id'])}
                     for a in result['affected']]
            is_batch = len(result['affected']) > 1
            desc = f'Accepted prediction: added "{result["species"]}"'
            if is_batch:
                desc += f' to {len(result["affected"])} photos'
            db.record_edit('prediction_accept', desc, str(result['keyword_id']),
                           items, is_batch=is_batch)
        return jsonify({"ok": True})

    @app.route("/api/predictions/<int:pred_id>/reject", methods=["POST"])
    def api_reject_prediction(pred_id):
        db = _get_db()
        pred = db.conn.execute(
            """SELECT pr.id, pr.species, pr.detection_id, pr.model, d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE pr.id = ?""",
            (pred_id,),
        ).fetchone()
        db.update_prediction_status(pred_id, "rejected")
        if pred:
            # Also reject sibling alternative predictions
            db.conn.execute(
                """UPDATE predictions SET status = 'rejected'
                   WHERE detection_id = ? AND model = ? AND id != ? AND status = 'alternative'""",
                (pred["detection_id"], pred["model"], pred_id),
            )
            db.conn.commit()
            db.record_edit('prediction_reject',
                           f'Rejected prediction "{pred["species"]}"',
                           'rejected',
                           [{'photo_id': pred['photo_id'], 'old_value': 'pending', 'new_value': 'rejected'}])
        return jsonify({"ok": True})

    @app.route("/api/predictions/group/<group_id>")
    def api_prediction_group(group_id):
        """Get all predictions and photo data for a burst group."""
        db = _get_db()
        preds = db.get_group_predictions(group_id)
        return jsonify([dict(p) for p in preds])

    @app.route("/api/predictions/group/apply", methods=["POST"])
    def api_prediction_group_apply():
        """Apply pick/reject decisions and species to a burst group."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        picks = body.get("picks", [])  # list of photo_ids
        rejects = body.get("rejects", [])  # list of photo_ids
        removed = body.get("removed", [])  # list of prediction_ids to ungroup
        species = body.get("species", "")

        # Capture old flag values before mutation
        all_flag_pids = picks + rejects
        old_flags = {}
        for pid in all_flag_pids:
            old = db.get_photo(pid)
            if old:
                old_flags[pid] = old["flag"] or "none"

        # Flag picks and add species keyword
        if species:
            kid = db.add_keyword(species, is_species=True)
            for pid in picks:
                db.update_photo_flag(pid, "flagged")
                db.tag_photo(pid, kid)
                db.queue_change(pid, "keyword_add", species)

            # Record keyword_add history for picks
            kw_items = [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)}
                        for pid in picks]
            if kw_items:
                db.record_edit('keyword_add',
                               f'Added "{species}" to {len(picks)} photos (group prediction)',
                               str(kid), kw_items, is_batch=len(picks) > 1)
        else:
            # No species — still flag picks
            for pid in picks:
                db.update_photo_flag(pid, "flagged")

        # Reject rejects
        for pid in rejects:
            db.update_photo_flag(pid, "rejected")

        # Record flag history for all picks + rejects
        flag_items = []
        for pid in picks:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'flagged'})
        for pid in rejects:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'rejected'})
        if flag_items:
            desc = f'Group prediction: flagged {len(picks)}, rejected {len(rejects)}'
            db.record_edit('flag', desc, 'group_apply', flag_items, is_batch=True)

        # Mark all predictions in this group as accepted/rejected
        for pid in picks:
            db.update_predictions_status_by_photo(pid, 'accepted')
        for pid in rejects:
            db.update_predictions_status_by_photo(pid, 'rejected')

        # Remove predictions from group
        for pred_id in removed:
            db.ungroup_prediction(pred_id)
        return jsonify({"ok": True})

    # -- Detection API routes --

    @app.route("/api/detections/<int:photo_id>")
    def api_detections(photo_id):
        """Get all detections for a photo."""
        db = _get_db()
        dets = db.get_detections(photo_id)
        return jsonify([dict(d) for d in dets])

    @app.route("/api/classify/readiness")
    def api_classify_readiness():
        """Check what's ready for classification and what will need work."""
        from classifier import _embedding_cache_path
        from labels import get_active_labels, get_saved_labels, load_merged_labels
        from models import get_active_model, get_models

        model_id = request.args.get("model_id", "")
        labels_file = request.args.get("labels_file", "")
        labels_files = request.args.getlist("labels_files")

        # Resolve model
        models = get_models()
        model = None
        if model_id:
            model = next((m for m in models if m["id"] == model_id), None)
        if not model:
            model = get_active_model()

        model_ready = bool(model and model.get("downloaded"))
        model_name = model["name"] if model else "None"
        model_size = model.get("size_mb", 0) if model else 0
        model_source = model.get("source", "") if model else ""
        model_type = model.get("model_type", "bioclip") if model else "bioclip"
        needs_download = not model_ready and (
            model_source.startswith("hf-hub:") or model_source == "timm"
        )

        import shutil

        exiftool_status = {
            "installed": shutil.which("exiftool") is not None,
            "brew_available": shutil.which("brew") is not None,
        }

        # timm models have a fixed class set — no labels needed
        if model_type == "timm":
            return jsonify(
                {
                    "model_name": model_name,
                    "model_ready": model_ready,
                    "model_size_mb": model_size,
                    "needs_download": needs_download,
                    "labels_name": "Built-in (10K iNat21 species)",
                    "labels_count": 10000,
                    "use_tol": False,
                    "embeddings_cached": True,  # no embeddings to compute
                    "exiftool": exiftool_status,
                }
            )

        # Resolve labels (BioCLIP path)
        use_tol = False
        label_count = 0
        label_name = ""
        labels = []

        if labels_file:
            # Single file override from query param (classify page picker)
            if os.path.exists(labels_file):
                with open(labels_file) as f:
                    label_count = sum(1 for line in f if line.strip())
                for ls in get_saved_labels():
                    if ls.get("labels_file") == labels_file:
                        label_name = ls.get("name", labels_file)
                        break
                with open(labels_file) as f:
                    labels = [line.strip() for line in f if line.strip()]
        elif labels_files:
            # Multiple files override from query param
            active_sets = []
            saved = get_saved_labels()
            saved_by_file = {s["labels_file"]: s for s in saved}
            for p in labels_files:
                meta = saved_by_file.get(p, {"labels_file": p})
                active_sets.append(meta)
            labels = load_merged_labels(active_sets)
            label_count = len(labels)
            names = [s.get("name", os.path.basename(s["labels_file"])) for s in active_sets]
            label_name = ", ".join(names)
        else:
            db = _get_db()
            ws_labels = db.get_workspace_active_labels()
            if ws_labels is not None:
                saved_by_file = {s["labels_file"]: s for s in get_saved_labels()}
                active_sets = [saved_by_file.get(p, {"labels_file": p}) for p in ws_labels if os.path.exists(p)]
            else:
                active_sets = get_active_labels()
            if active_sets:
                labels = load_merged_labels(active_sets)
                label_count = len(labels)
                names = [s.get("name", os.path.basename(s["labels_file"])) for s in active_sets]
                label_name = ", ".join(names)
            else:
                tol_models = {"hf-hub:imageomics/bioclip", "hf-hub:imageomics/bioclip-2"}
                model_str_check = model.get("model_str", "") if model else ""
                if model_str_check in tol_models:
                    use_tol = True
                    label_name = "Tree of Life (all species)"
                else:
                    label_name = "No labels — download a species list in Settings"

        # Check embedding cache
        embeddings_cached = False
        if model and not use_tol and labels:
            cache_path = _embedding_cache_path(labels, model.get("model_str", ""))
            embeddings_cached = os.path.exists(cache_path)

        return jsonify(
            {
                "model_name": model_name,
                "model_ready": model_ready,
                "model_size_mb": model_size,
                "needs_download": needs_download,
                "labels_name": label_name,
                "labels_count": label_count,
                "use_tol": use_tol,
                "embeddings_cached": embeddings_cached,
                "exiftool": exiftool_status,
            }
        )

    @app.route("/api/system/install-exiftool", methods=["POST"])
    def api_install_exiftool():
        """Install exiftool via Homebrew."""
        import shutil
        import subprocess

        if shutil.which("exiftool"):
            return jsonify({"success": True, "message": "exiftool is already installed"})

        if not shutil.which("brew"):
            return jsonify({
                "success": False,
                "error": "Homebrew is not installed. Install it from https://brew.sh, then run: brew install exiftool",
            })

        try:
            result = subprocess.run(
                ["brew", "install", "exiftool"],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0:
                return jsonify({"success": True, "message": "exiftool installed successfully"})
            else:
                return jsonify({
                    "success": False,
                    "error": f"brew install failed: {result.stderr[:500]}",
                })
        except subprocess.TimeoutExpired:
            return jsonify({"success": False, "error": "Installation timed out after 5 minutes"})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    @app.route("/api/classify/config")
    def api_classify_config():
        """Return classifier configuration from model registry."""
        import config as cfg
        from models import get_active_model, get_taxonomy_info

        active = get_active_model()
        tax = get_taxonomy_info()
        user_cfg = _get_db().get_effective_config(cfg.load())
        return jsonify(
            {
                "model_name": active["name"] if active else "No model",
                "model_str": active["model_str"] if active else "",
                "weights_path": active["weights_path"] if active else "",
                "weights_available": active["downloaded"] if active else False,
                "taxonomy_available": tax["available"],
                "taxonomy_species_count": init_db.count_keywords(),
                "default_threshold": user_cfg["classification_threshold"],
                "default_grouping_window": user_cfg["grouping_window_seconds"],
                "default_similarity_threshold": user_cfg.get(
                    "similarity_threshold", 0.85
                ),
            }
        )

    @app.route("/api/config")
    def api_config_get():
        import config as cfg

        return jsonify(cfg.load())

    @app.route("/api/config", methods=["POST"])
    def api_config_set():
        import config as cfg

        body = request.get_json(silent=True) or {}
        current = cfg.load()

        # Handle keyboard_shortcuts with validation
        if "keyboard_shortcuts" in body:
            shortcuts = body["keyboard_shortcuts"]
            if isinstance(shortcuts, dict):
                valid_contexts = cfg.DEFAULTS["keyboard_shortcuts"]
                validated = {}
                for ctx_name, actions in shortcuts.items():
                    if ctx_name in valid_contexts and isinstance(actions, dict):
                        validated[ctx_name] = {}
                        for action, key_str in actions.items():
                            if action in valid_contexts[ctx_name] and isinstance(key_str, str):
                                validated[ctx_name][action] = key_str.strip().lower()
                current["keyboard_shortcuts"] = cfg._deep_merge(
                    cfg.DEFAULTS["keyboard_shortcuts"], validated
                )

        for key in body:
            if key == "keyboard_shortcuts":
                continue
            if key in cfg.DEFAULTS:
                current[key] = body[key]
        # Apply HF token to environment immediately
        hf_token = current.get("hf_token", "")
        if hf_token:
            os.environ["HF_TOKEN"] = hf_token
        elif "HF_TOKEN" in os.environ:
            del os.environ["HF_TOKEN"]
        cfg.save(current)
        return jsonify({"ok": True})

    @app.route("/api/darktable/status")
    def api_darktable_status():
        import config as cfg
        from develop import find_darktable

        configured = cfg.get("darktable_bin")
        binary = find_darktable(configured)
        return jsonify({
            "available": binary is not None,
            "bin": binary or "",
            "configured_bin": configured,
            "style": cfg.get("darktable_style"),
            "output_format": cfg.get("darktable_output_format"),
            "output_dir": cfg.get("darktable_output_dir"),
        })

    @app.route("/api/photos/open-external", methods=["POST"])
    def api_photos_open_external():
        import subprocess
        import sys

        import config as cfg

        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids")
        if not isinstance(photo_ids, list) or not photo_ids:
            return jsonify({"error": "photo_ids required"}), 400
        if not all(isinstance(pid, int) for pid in photo_ids):
            return jsonify({"error": "photo_ids must be a list of integers"}), 400

        db = _get_db()
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
        file_paths = []
        for pid in photo_ids:
            photo = db.get_photo(pid)
            if not photo:
                continue
            folder_path = folders.get(photo["folder_id"], "")
            if folder_path:
                file_paths.append(os.path.join(folder_path, photo["filename"]))

        if not file_paths:
            return jsonify({"error": "No photos found"}), 404

        editor = cfg.get("external_editor")
        try:
            if editor:
                if sys.platform == "darwin" and editor.endswith(".app"):
                    subprocess.Popen(["open", "-a", editor] + file_paths)
                else:
                    subprocess.Popen([editor] + file_paths)
            else:
                if sys.platform == "darwin":
                    subprocess.Popen(["open"] + file_paths)
                elif sys.platform == "win32":
                    for fp in file_paths:
                        os.startfile(fp)
                else:
                    for fp in file_paths:
                        subprocess.Popen(["xdg-open", fp])
        except Exception as e:
            log.warning("Failed to open external editor: %s", e)
            return jsonify({"error": str(e)}), 500

        return jsonify({"opened": len(file_paths)})

    @app.route("/api/storage")
    def api_storage():
        """Comprehensive storage info for the storage management panel."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR
        from models import DEFAULT_MODELS_DIR

        def _dir_stats(path):
            count = 0
            total = 0
            if os.path.isdir(path):
                for f in os.listdir(path):
                    fp = os.path.join(path, f)
                    if os.path.isfile(fp):
                        count += 1
                        total += os.path.getsize(fp)
            return {"count": count, "size": total, "path": path}

        def _dir_size_recursive(path):
            total = 0
            if os.path.isdir(path):
                for dirpath, dirnames, filenames in os.walk(path):
                    for f in filenames:
                        total += os.path.getsize(os.path.join(dirpath, f))
            return total

        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        thumb = _dir_stats(app.config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(
            os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
        )
        preview = _dir_stats(preview_dir)
        emb = _dir_stats(EMB_CACHE_DIR)
        models_size = _dir_size_recursive(DEFAULT_MODELS_DIR)

        # HuggingFace cache — only count Vireo-relevant models
        hf_cache = os.path.expanduser("~/.cache/huggingface/hub")
        hf_size = 0
        hf_models = []
        if os.path.isdir(hf_cache):
            for d in os.listdir(hf_cache):
                if d.startswith("models--imageomics") or d.startswith(
                    "models--bioclip"
                ):
                    dp = os.path.join(hf_cache, d)
                    size = _dir_size_recursive(dp)
                    hf_size += size
                    hf_models.append(
                        {
                            "name": d.replace("models--", "").replace("--", "/"),
                            "size": size,
                        }
                    )

        total = (
            db_size
            + thumb["size"]
            + preview["size"]
            + emb["size"]
            + models_size
            + hf_size
        )

        return jsonify(
            {
                "total": total,
                "database": {"size": db_size, "path": db_path},
                "thumbnails": thumb,
                "previews": preview,
                "embeddings": emb,
                "models": {"size": models_size, "path": DEFAULT_MODELS_DIR},
                "hf_cache": {"size": hf_size, "path": hf_cache, "models": hf_models},
            }
        )

    @app.route("/api/storage/files")
    def api_storage_files():
        """List individual files in a cache directory."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR

        cache_type = request.args.get("type", "")
        dirs = {
            "thumbnails": app.config["THUMB_CACHE_DIR"],
            "previews": os.path.join(
                os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
            ),
            "embeddings": EMB_CACHE_DIR,
        }
        cache_dir = dirs.get(cache_type)
        if not cache_dir:
            return json_error("Unknown cache type")

        # Load embedding manifest for display names
        manifest = {}
        if cache_type == "embeddings":
            from classifier import _load_manifest
            manifest = _load_manifest()

        files = []
        if os.path.isdir(cache_dir):
            for f in sorted(os.listdir(cache_dir)):
                fp = os.path.join(cache_dir, f)
                if os.path.isfile(fp) and f != "manifest.json":
                    entry = {"name": f, "size": os.path.getsize(fp)}
                    if f in manifest:
                        entry["meta"] = manifest[f]
                    files.append(entry)
        return jsonify({"type": cache_type, "path": cache_dir, "files": files})

    @app.route("/api/storage/clear", methods=["POST"])
    def api_storage_clear():
        """Clear a specific cache."""
        import shutil

        body = request.get_json(silent=True) or {}
        cache_type = body.get("type", "")

        if cache_type == "previews":
            preview_dir = os.path.join(
                os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
            )
            if os.path.isdir(preview_dir):
                shutil.rmtree(preview_dir)
                log.info("Preview cache cleared")
            return jsonify({"ok": True})
        elif cache_type == "thumbnails":
            thumb_dir = app.config["THUMB_CACHE_DIR"]
            if os.path.isdir(thumb_dir):
                shutil.rmtree(thumb_dir)
                log.info("Thumbnail cache cleared")
            return jsonify({"ok": True})
        elif cache_type == "embeddings":
            from classifier import CACHE_DIR

            if os.path.isdir(CACHE_DIR):
                shutil.rmtree(CACHE_DIR)
                log.info("Embedding cache cleared")
            return jsonify({"ok": True})
        else:
            return json_error("Unknown cache type")

    @app.route("/api/storage/delete-files", methods=["POST"])
    def api_storage_delete_files():
        """Delete specific files from a cache directory."""
        from classifier import CACHE_DIR as EMB_CACHE_DIR

        body = request.get_json(silent=True) or {}
        cache_type = body.get("type", "")
        filenames = body.get("files", [])
        dirs = {
            "thumbnails": app.config["THUMB_CACHE_DIR"],
            "previews": os.path.join(
                os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
            ),
            "embeddings": EMB_CACHE_DIR,
        }
        cache_dir = dirs.get(cache_type)
        if not cache_dir:
            return json_error("Unknown cache type")
        if not filenames:
            return json_error("No files specified")

        deleted = 0
        for fname in filenames:
            # Prevent path traversal
            safe = os.path.basename(fname)
            fp = os.path.join(cache_dir, safe)
            if os.path.isfile(fp):
                os.remove(fp)
                deleted += 1
        log.info("Deleted %d files from %s cache", deleted, cache_type)
        return jsonify({"ok": True, "deleted": deleted})

    @app.route("/api/preview-cache")
    def api_preview_cache():
        """Return info about the preview image cache."""
        preview_dir = os.path.join(
            os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
        )
        count = 0
        total_size = 0
        if os.path.isdir(preview_dir):
            for f in os.listdir(preview_dir):
                fp = os.path.join(preview_dir, f)
                if os.path.isfile(fp):
                    count += 1
                    total_size += os.path.getsize(fp)
        return jsonify({"count": count, "total_size": total_size})

    @app.route("/api/embedding-cache")
    def api_embedding_cache():
        """Return info about cached label embeddings."""
        from classifier import CACHE_DIR

        if not os.path.isdir(CACHE_DIR):
            return jsonify({"entries": [], "total_size": 0})
        entries = []
        total_size = 0
        for f in sorted(os.listdir(CACHE_DIR)):
            if f.endswith(".npy") or f.endswith(".pt"):
                fp = os.path.join(CACHE_DIR, f)
                size = os.path.getsize(fp)
                total_size += size
                entries.append({"file": f, "size": size})
        return jsonify({"entries": entries, "total_size": total_size})

    @app.route("/api/embedding-matrix")
    def api_embedding_matrix():
        """Return which model+labels combinations have cached embeddings."""
        from classifier import _embedding_cache_path
        from labels import get_saved_labels
        from models import get_models

        models = [m for m in get_models() if m["downloaded"]]
        label_sets = get_saved_labels()

        matrix = []
        for ls in label_sets:
            labels_file = ls.get("labels_file", "")
            if not labels_file or not os.path.exists(labels_file):
                continue
            with open(labels_file) as f:
                labels = [line.strip() for line in f if line.strip()]
            row = {
                "labels_name": ls.get("name", ""),
                "labels_file": labels_file,
                "species_count": len(labels),
                "models": {},
            }
            for m in models:
                cache_path = _embedding_cache_path(labels, m["model_str"])
                row["models"][m["id"]] = {
                    "cached": os.path.exists(cache_path),
                    "model_name": m["name"],
                }
            matrix.append(row)

        return jsonify(
            {
                "models": [{"id": m["id"], "name": m["name"]} for m in models],
                "matrix": matrix,
            }
        )

    @app.route("/api/jobs/precompute-embeddings", methods=["POST"])
    def api_job_precompute_embeddings():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        labels_file = body.get("labels_file")
        if not model_id or not labels_file:
            return json_error("model_id and labels_file required")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from classifier import Classifier
            from models import get_models

            # Find the model
            models = get_models()
            model = None
            for m in models:
                if m["id"] == model_id:
                    model = m
                    break
            if not model or not model["downloaded"]:
                raise RuntimeError(f"Model {model_id} not found or not downloaded")

            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": f'Loading {model["name"]} and computing embeddings...',
                    "rate": 0,
                },
            )

            with open(labels_file) as f:
                labels = [line.strip() for line in f if line.strip()]

            log.info(
                "Pre-computing embeddings: %d labels with %s",
                len(labels),
                model["name"],
            )

            def _progress(current, total):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": f"Computing embeddings ({current}/{total} labels)…",
                        "rate": 0,
                    },
                )

            # This will compute and cache the embeddings
            Classifier(
                labels=labels,
                model_str=model["model_str"],
                pretrained_str=model["weights_path"],
                embedding_progress_callback=_progress,
            )

            return {"labels": len(labels), "model": model["name"]}

        job_id = runner.start(
            "precompute-embeddings",
            work,
            config={
                "model_id": model_id,
                "labels_file": labels_file,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/embedding-cache", methods=["DELETE"])
    def api_embedding_cache_clear():
        """Clear all cached label embeddings."""
        import shutil

        from classifier import CACHE_DIR

        if os.path.isdir(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            log.info("Embedding cache cleared")
        return jsonify({"ok": True})

    @app.route("/api/version")
    def api_version():
        try:
            from importlib.metadata import version as pkg_version
            ver = pkg_version("vireo")
        except Exception:
            import tomllib
            try:
                with open(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"), "rb") as f:
                    ver = tomllib.load(f)["project"]["version"]
            except Exception:
                ver = "0.0.0"
        return jsonify({"version": ver})

    @app.route("/api/volumes", methods=["GET"])
    def api_volumes():
        """List mounted volumes (macOS/Linux) to help find SD cards."""
        import platform
        volumes = []
        if platform.system() == "Darwin":
            vol_dir = "/Volumes"
        else:
            vol_dir = "/media"
        if os.path.isdir(vol_dir):
            for name in sorted(os.listdir(vol_dir)):
                path = os.path.join(vol_dir, name)
                if os.path.isdir(path):
                    volumes.append({"name": name, "path": path})
        return jsonify(volumes)

    @app.route("/api/browse", methods=["GET"])
    def api_browse():
        """List subdirectories at a given path for folder browser."""
        path = request.args.get("path", os.path.expanduser("~"))
        if not os.path.isdir(path):
            return json_error("path is not a valid directory")
        dirs = []
        try:
            for name in sorted(os.listdir(path), key=str.casefold):
                if name.startswith("."):
                    continue
                full = os.path.join(path, name)
                if os.path.isdir(full):
                    dirs.append({"name": name, "path": full})
        except PermissionError:
            return json_error("permission denied", 403)
        return jsonify({"path": path, "dirs": dirs})

    @app.route("/api/browse/photo-counts", methods=["POST"])
    def api_browse_photo_counts():
        """Return recursive photo-file counts for a list of folder paths.

        Used by the folder browser to show per-folder counts next to each
        subfolder so users can see which folders contain photos before
        selecting one.
        """
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        file_types = body.get("file_types", [])
        if not isinstance(paths, list):
            return json_error("paths must be a list", 400)

        from ingest import discover_source_files

        ft = file_types if file_types else "both"
        counts = {}
        for p in paths:
            # Non-string entries (dicts, lists, numbers) can't be dict keys
            # and aren't valid paths — skip them rather than 500.
            if not isinstance(p, str):
                continue
            if not os.path.isdir(p):
                counts[p] = 0
                continue
            try:
                discovered = discover_source_files(p, file_types=ft, recursive=True)
                counts[p] = len(discovered)
            except (OSError, PermissionError):
                counts[p] = 0
        return jsonify({"counts": counts})

    @app.route("/api/browse/mkdir", methods=["POST"])
    def api_browse_mkdir():
        """Create a new directory."""
        body = request.get_json(silent=True) or {}
        path = body.get("path", "")
        if not path:
            return json_error("path is required")
        if not os.path.isabs(path):
            return json_error("path must be absolute")
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as e:
            return json_error(str(e), 500)
        return jsonify({"name": os.path.basename(path), "path": path})

    # -- Move rules API --

    @app.route("/api/move-rules", methods=["GET"])
    def api_list_move_rules():
        db = _get_db()
        rules = db.list_move_rules()
        return jsonify([dict(r) for r in rules])

    @app.route("/api/move-rules", methods=["POST"])
    def api_create_move_rule():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        destination = body.get("destination", "").strip()
        criteria = body.get("criteria", {})
        if not name or not destination:
            return json_error("name and destination required")
        rule_id = db.create_move_rule(name, destination, criteria)
        return jsonify({"ok": True, "id": rule_id})

    @app.route("/api/move-rules/<int:rule_id>", methods=["PUT"])
    def api_update_move_rule(rule_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        kwargs = {}
        if "name" in body:
            kwargs["name"] = body["name"]
        if "destination" in body:
            kwargs["destination"] = body["destination"]
        if "criteria" in body:
            kwargs["criteria"] = body["criteria"]
        db.update_move_rule(rule_id, **kwargs)
        return jsonify({"ok": True})

    @app.route("/api/move-rules/<int:rule_id>", methods=["DELETE"])
    def api_delete_move_rule(rule_id):
        db = _get_db()
        db.delete_move_rule(rule_id)
        return jsonify({"ok": True})

    @app.route("/api/move-rules/preview", methods=["POST"])
    def api_move_rule_preview():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        criteria = body.get("criteria", {})
        photo_ids = db.query_move_rule_matches(criteria)
        return jsonify({"count": len(photo_ids), "photo_ids": photo_ids})

    # -- Import API routes --

    @app.route("/api/import/preview", methods=["POST"])
    def api_import_preview():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        if not catalogs:
            return json_error("catalogs required")
        try:
            from importer import preview_import

            result = preview_import(catalogs, db)
            return jsonify(result)
        except Exception as e:
            return json_error(str(e), 500)

    @app.route("/api/import/folder-preview", methods=["POST"])
    def api_import_folder_preview():
        """Discover files in source folders and return metadata for preview."""
        body = request.get_json(silent=True) or {}
        folders = body.get("folders", [])
        file_types = body.get("file_types", [])
        if not folders:
            return json_error("folders required", 400)

        from ingest import discover_source_files

        all_files = []
        multi_source = len(folders) > 1

        # Compute unique display names for each source folder.
        # Use shortest trailing path segments that are unique across
        # all sources (e.g. /mnt/cardA/DCIM and /mnt/cardB/DCIM
        # become cardA/DCIM and cardB/DCIM).
        root_names = {}
        if multi_source:
            parts = [Path(f).parts for f in folders]
            for depth in range(1, max(len(p) for p in parts) + 1):
                suffixes = [str(Path(*p[-depth:])) for p in parts]
                if len(set(suffixes)) == len(suffixes):
                    for folder_path, suffix in zip(folders, suffixes, strict=True):
                        root_names[folder_path] = suffix
                    break
            else:
                for folder_path in folders:
                    root_names[folder_path] = folder_path

        for folder in folders:
            root_name = root_names.get(folder, os.path.basename(folder.rstrip("/")))
            discovered = discover_source_files(folder, file_types=file_types if file_types else "both", recursive=body.get("recursive", True))
            for f in discovered:
                stat = f.stat()
                # Determine subfolder relative to the source root
                try:
                    rel = f.parent.relative_to(folder)
                    subfolder = str(rel) if str(rel) != "." else root_name
                except ValueError:
                    subfolder = root_name
                # Prefix with source root name when multiple sources to
                # prevent collisions (e.g. two cards with DCIM/100CANON)
                if multi_source and subfolder != root_name:
                    subfolder = os.path.join(root_name, subfolder)

                all_files.append({
                    "path": str(f),
                    "filename": f.name,
                    "subfolder": subfolder,
                    "size": stat.st_size,
                    "extension": f.suffix.lower(),
                    "mtime": stat.st_mtime,
                })

        # Build summary
        type_breakdown = {}
        total_size = 0
        for f in all_files:
            ext = f["extension"]
            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += f["size"]

        return jsonify({
            "total_count": len(all_files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": all_files,
        })

    @app.route("/api/import/folder-preview/thumbnail")
    def api_import_folder_preview_thumbnail():
        """Generate an on-the-fly thumbnail for a source file (not yet imported)."""
        file_path = request.args.get("path", "")
        if not file_path:
            return json_error("path parameter required", 400)
        if not os.path.isfile(file_path):
            return "", 404

        from image_loader import load_image
        img = load_image(file_path, max_size=200)
        if img is None:
            return "", 404

        import io
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=70)
        buf.seek(0)

        resp = make_response(buf.read())
        resp.content_type = "image/jpeg"
        resp.cache_control.public = True
        resp.cache_control.max_age = 300  # 5 min — these are ephemeral
        return resp

    # -- Audit API routes --

    @app.route("/api/audit/drift")
    def api_audit_drift():
        db = _get_db()
        from audit import check_drift

        return jsonify(check_drift(db))

    @app.route("/api/audit/orphans")
    def api_audit_orphans():
        db = _get_db()
        from audit import check_orphans

        return jsonify(check_orphans(db))

    @app.route("/api/audit/untracked")
    def api_audit_untracked():
        db = _get_db()
        body = request.args.getlist("root") or []
        from audit import check_untracked

        return jsonify(check_untracked(db, body))

    @app.route("/api/audit/resolve", methods=["POST"])
    def api_audit_resolve():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_id = body.get("photo_id")
        direction = body.get("direction")
        from audit import resolve_drift

        resolve_drift(db, photo_id, direction)
        return jsonify({"ok": True})

    @app.route("/api/audit/resolve-all", methods=["POST"])
    def api_audit_resolve_all():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        direction = body.get("direction")
        from audit import check_drift, resolve_drift

        drifts = check_drift(db)
        for d in drifts:
            resolve_drift(db, d["photo_id"], direction)
        return jsonify({"ok": True, "resolved": len(drifts)})

    @app.route("/api/audit/remove-orphans", methods=["POST"])
    def api_audit_remove_orphans():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        from audit import remove_orphans

        remove_orphans(db, photo_ids)
        return jsonify({"ok": True, "removed": len(photo_ids)})

    @app.route("/api/audit/import-untracked", methods=["POST"])
    def api_audit_import_untracked():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        from audit import import_untracked

        import_untracked(db, paths)
        return jsonify({"ok": True, "imported": len(paths)})

    # -- Scan status (kept, non-job) --

    # -- Model & Taxonomy API routes --

    @app.route("/api/models")
    def api_models():
        from models import get_active_model, get_models

        active = get_active_model()
        return jsonify(
            {
                "models": get_models(),
                "active_id": active["id"] if active else None,
            }
        )

    @app.route("/api/models/<model_id>", methods=["DELETE"])
    def api_remove_model(model_id):
        """Remove a model's weights from disk and unregister it."""
        from models import remove_model

        removed = remove_model(model_id)
        if removed:
            log.info("Removed model: %s", model_id)
            return jsonify({"ok": True})
        return json_error("Model not found", 404)

    @app.route("/api/models/active", methods=["POST"])
    def api_set_active_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")
        from models import set_active_model

        set_active_model(model_id)
        return jsonify({"ok": True})

    @app.route("/api/models/custom", methods=["POST"])
    def api_add_custom_model():
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        weights_path = body.get("weights_path", "").strip()
        model_str = body.get("model_str", "ViT-B-16")
        if not name or not weights_path:
            return json_error("name and weights_path required")
        from models import register_model

        model_id = "custom-" + name.lower().replace(" ", "-")
        register_model(model_id, name, model_str, weights_path, "Custom model")
        return jsonify({"ok": True, "model_id": model_id})

    @app.route("/api/jobs/download-model", methods=["POST"])
    def api_job_download_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from models import download_model

            def progress_cb(msg, current=0, total=0, rate=0):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = msg
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": msg,
                        "rate": rate,
                        "phase": "Downloading model",
                    },
                )

            path = download_model(model_id, progress_callback=progress_cb)
            return {"model_id": model_id, "weights_path": path}

        job_id = runner.start("download-model", work, config={"model_id": model_id}, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/download-hf-model", methods=["POST"])
    def api_job_download_hf_model():
        body = request.get_json(silent=True) or {}
        repo_id = body.get("repo_id", "").strip()
        if not repo_id:
            return json_error("repo_id required")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from models import download_hf_model

            def progress_cb(msg):
                job["progress"]["current_file"] = msg
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            result = download_hf_model(repo_id, progress_callback=progress_cb)
            return result

        job_id = runner.start("download-model", work, config={"repo_id": repo_id}, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/taxonomy/info")
    def api_taxonomy_info():
        from models import get_taxonomy_info

        return jsonify(get_taxonomy_info())

    @app.route("/api/jobs/download-taxonomy", methods=["POST"])
    def api_job_download_taxonomy():
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from taxonomy import download_taxonomy

            def progress_cb(msg):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
            download_taxonomy(taxonomy_path, progress_callback=progress_cb)
            return {"ok": True}

        job_id = runner.start("download-taxonomy", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    # -- Labels API routes --

    @app.route("/api/labels/search-places")
    def api_labels_search_places():
        q = request.args.get("q", "")
        if len(q) < 2:
            return jsonify([])
        from labels import search_places

        return jsonify(search_places(q))

    @app.route("/api/labels/taxon-groups")
    def api_labels_taxon_groups():
        from labels import TAXON_GROUPS

        return jsonify([{"key": k, "name": v["name"]} for k, v in TAXON_GROUPS.items()])

    @app.route("/api/labels/observation-filters")
    def api_labels_observation_filters():
        from labels import OBSERVATION_FILTERS

        return jsonify([
            {"key": k, "name": v["name"], "description": v["description"]}
            for k, v in OBSERVATION_FILTERS.items()
        ])

    @app.route("/api/labels")
    def api_labels_list():
        from labels import get_active_labels as get_global_active_labels
        from labels import get_saved_labels

        db = _get_db()
        saved = get_saved_labels()
        ws_labels = db.get_workspace_active_labels()
        if ws_labels is not None:
            # Resolve workspace labels to metadata
            saved_by_file = {s["labels_file"]: s for s in saved}
            active = []
            for p in ws_labels:
                if os.path.exists(p):
                    meta = saved_by_file.get(p, {"labels_file": p})
                    active.append(meta)
        else:
            active = get_global_active_labels()
        return jsonify(
            {
                "labels": saved,
                "active": active,
            }
        )

    @app.route("/api/labels", methods=["DELETE"])
    def api_delete_labels():
        from labels import delete_labels

        body = request.get_json(silent=True) or {}
        labels_file = body.get("labels_file")
        if not labels_file:
            return json_error("labels_file required")
        delete_labels(labels_file)
        return jsonify({"ok": True})

    @app.route("/api/labels/active", methods=["POST"])
    def api_set_active_labels():
        body = request.get_json(silent=True) or {}
        # Accept new list format or old single-path format
        labels_files = body.get("labels_files")
        if labels_files is None:
            single = body.get("labels_file")
            if not single:
                return json_error("labels_files or labels_file required")
            labels_files = [single]
        db = _get_db()
        db.set_workspace_active_labels(labels_files)
        return jsonify({"ok": True})

    @app.route("/api/jobs/fetch-labels", methods=["POST"])
    def api_job_fetch_labels():
        body = request.get_json(silent=True) or {}
        place_id = body.get("place_id")
        place_name = body.get("place_name", "")
        taxon_groups = body.get("taxon_groups", ["birds"])
        observation_filter = body.get("observation_filter", "research")
        name = body.get("name", "")
        if not place_id:
            return json_error("place_id required")
        if observation_filter not in ("research", "wild", "all"):
            observation_filter = "research"
        if not name:
            from labels import OBSERVATION_FILTERS
            group_names = ", ".join(g.title() for g in taxon_groups)
            filter_label = OBSERVATION_FILTERS[observation_filter]["name"]
            name = f"{place_name} {group_names} ({filter_label})".strip()

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from labels import fetch_species_list, save_labels

            def progress_cb(msg, current=None, total=None):
                job["progress"]["current_file"] = msg
                if current is not None:
                    job["progress"]["current"] = current
                if total is not None:
                    job["progress"]["total"] = total
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current or 0,
                        "total": total or 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            species = fetch_species_list(
                place_id, taxon_groups,
                observation_filter=observation_filter,
                progress_callback=progress_cb,
            )
            if not species:
                raise RuntimeError(
                    "No species found for this region and taxa selection"
                )
            labels_path = save_labels(
                name, place_id, place_name, taxon_groups, species,
                observation_filter=observation_filter,
            )
            from db import Database
            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            thread_db.set_workspace_active_labels([labels_path])

            # Auto-compute embeddings for the active model
            from models import get_active_model

            active_model = get_active_model()
            if active_model and active_model["downloaded"]:
                try:
                    from classifier import _embedding_cache_path

                    cache_path = _embedding_cache_path(
                        list(set(species)), active_model["model_str"]
                    )
                    if not os.path.exists(cache_path):
                        progress_cb(
                            f'Pre-computing embeddings for {active_model["name"]}...',
                            0,
                            0,
                        )
                        from classifier import Classifier

                        Classifier(
                            labels=list(set(species)),
                            model_str=active_model["model_str"],
                            pretrained_str=active_model["weights_path"],
                        )
                        progress_cb("Embeddings cached!", 0, 0)
                except Exception:
                    log.warning(
                        "Auto-compute embeddings failed (non-fatal)", exc_info=True
                    )

            return {"species_count": len(set(species)), "labels_file": labels_path}

        job_id = runner.start(
            "fetch-labels",
            work,
            config={
                "place_id": place_id,
                "place_name": place_name,
                "taxon_groups": taxon_groups,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    # -- iNaturalist --

    @app.route("/api/inat/prepare/<int:photo_id>")
    def api_inat_prepare(photo_id):
        """Prepare iNaturalist submission data for a photo."""
        import config as cfg

        db = _get_db()
        photo = db.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if not photo:
            return json_error("Photo not found", 404)

        pred = db.conn.execute(
            """SELECT pr.species, pr.scientific_name, pr.confidence
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ? AND d.workspace_id = ?
               ORDER BY pr.confidence DESC LIMIT 1""",
            (photo_id, db._active_workspace_id),
        ).fetchone()

        species = pred["species"] if pred else ""
        scientific = pred["scientific_name"] if pred else ""

        params = []
        if scientific:
            params.append("taxon_name=" + scientific)
        elif species:
            params.append("taxon_name=" + species)
        if photo["timestamp"]:
            params.append("observed_on=" + photo["timestamp"][:10])
        lat = photo["latitude"] if "latitude" in photo.keys() else None
        lng = photo["longitude"] if "longitude" in photo.keys() else None
        if lat and lng:
            params.append("lat=" + str(lat))
            params.append("lng=" + str(lng))

        upload_url = "https://www.inaturalist.org/observations/upload"
        if params:
            upload_url += "?" + "&".join(params)

        # Check submission history
        subs = db.get_inat_submissions([photo_id])
        already = photo_id in subs

        user_cfg = cfg.load()
        mode = "direct" if user_cfg.get("inat_token") else "quick"

        return jsonify({
            "species": species,
            "scientific_name": scientific,
            "confidence": pred["confidence"] if pred else 0,
            "timestamp": photo["timestamp"],
            "latitude": lat,
            "longitude": lng,
            "filename": photo["filename"],
            "upload_url": upload_url,
            "mode": mode,
            "already_submitted": already,
            "existing_observation_url": subs[photo_id]["observation_url"] if already else None,
        })

    @app.route("/api/inat/validate-token", methods=["POST"])
    def api_inat_validate_token():
        """Validate an iNaturalist API token."""
        import inat
        body = request.json or {}
        token = body.get("token", "")
        if not token:
            return json_error("Token is required")
        result = inat.validate_token(token)
        if result is None:
            return json_error("Invalid or expired token", 401)
        return jsonify(result)

    @app.route("/api/inat/submit", methods=["POST"])
    def api_inat_submit():
        """Submit a single observation to iNaturalist."""
        import config as cfg
        import inat

        user_cfg = cfg.load()
        token = user_cfg.get("inat_token")
        if not token:
            return json_error("iNaturalist token not configured. Add it in Settings.")

        data = request.json or {}
        photo_id = data.get("photo_id")
        if not photo_id:
            return json_error("photo_id is required")

        db = _get_db()
        photo = db.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if not photo:
            return json_error("Photo not found", 404)

        photo_path = os.path.join(photo["folder_path"], photo["filename"])
        if not os.path.isfile(photo_path):
            return json_error("Photo file not found on disk", 404)

        # Use overrides from request, or fall back to DB data
        pred = db.conn.execute(
            """SELECT pr.species, pr.scientific_name
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ? AND d.workspace_id = ?
               ORDER BY pr.confidence DESC LIMIT 1""",
            (photo_id, db._active_workspace_id),
        ).fetchone()

        taxon = data.get("taxon_name") or (pred["scientific_name"] if pred else None) or (pred["species"] if pred else None)
        observed_on = data.get("observed_on") or (photo["timestamp"][:10] if photo["timestamp"] else None)
        photo_lat = photo["latitude"] if "latitude" in photo.keys() else None
        photo_lng = photo["longitude"] if "longitude" in photo.keys() else None
        lat = data.get("latitude") if data.get("latitude") is not None else photo_lat
        lng = data.get("longitude") if data.get("longitude") is not None else photo_lng

        try:
            obs_id, obs_url = inat.submit_observation(
                token=token,
                photo_path=photo_path,
                taxon_name=taxon,
                observed_on=observed_on,
                latitude=lat,
                longitude=lng,
                description=data.get("description"),
                geoprivacy=data.get("geoprivacy", "open"),
            )
        except inat.InatAuthError as e:
            return json_error(str(e), 401)
        except inat.InatApiError as e:
            return json_error(str(e), 502)

        db.record_inat_submission(photo_id, obs_id, obs_url)
        return jsonify({"observation_id": obs_id, "observation_url": obs_url})

    @app.route("/api/inat/submit-batch", methods=["POST"])
    def api_inat_submit_batch():
        """Submit multiple observations to iNaturalist."""
        import config as cfg
        import inat

        user_cfg = cfg.load()
        token = user_cfg.get("inat_token")
        if not token:
            return json_error("iNaturalist token not configured. Add it in Settings.")

        submissions = (request.json or {}).get("submissions", [])
        if not submissions:
            return json_error("submissions array is required")

        db = _get_db()
        results = []
        for sub in submissions:
            photo_id = sub.get("photo_id")
            photo = db.conn.execute(
                """SELECT p.*, f.path as folder_path FROM photos p
                   JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
                (photo_id,),
            ).fetchone()
            if not photo:
                results.append({"photo_id": photo_id, "error": "Photo not found"})
                continue

            photo_path = os.path.join(photo["folder_path"], photo["filename"])
            if not os.path.isfile(photo_path):
                results.append({"photo_id": photo_id, "error": "Photo file not found on disk"})
                continue

            pred = db.conn.execute(
                """SELECT pr.species, pr.scientific_name
                   FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   WHERE d.photo_id = ? AND d.workspace_id = ?
                   ORDER BY pr.confidence DESC LIMIT 1""",
                (photo_id, db._active_workspace_id),
            ).fetchone()

            taxon = sub.get("taxon_name") or (pred["scientific_name"] if pred else None) or (pred["species"] if pred else None)
            observed_on = sub.get("observed_on") or (photo["timestamp"][:10] if photo["timestamp"] else None)
            photo_lat = photo["latitude"] if "latitude" in photo.keys() else None
            photo_lng = photo["longitude"] if "longitude" in photo.keys() else None
            lat = sub.get("latitude") if sub.get("latitude") is not None else photo_lat
            lng = sub.get("longitude") if sub.get("longitude") is not None else photo_lng

            try:
                obs_id, obs_url = inat.submit_observation(
                    token=token,
                    photo_path=photo_path,
                    taxon_name=taxon,
                    observed_on=observed_on,
                    latitude=lat,
                    longitude=lng,
                    description=sub.get("description"),
                    geoprivacy=sub.get("geoprivacy", "open"),
                )
                db.record_inat_submission(photo_id, obs_id, obs_url)
                results.append({"photo_id": photo_id, "observation_id": obs_id, "observation_url": obs_url})
            except (inat.InatAuthError, inat.InatApiError) as e:
                results.append({"photo_id": photo_id, "error": str(e)})

        return jsonify({"results": results})

    @app.route("/api/inat/submissions")
    def api_inat_submissions():
        """Return submission records for a set of photo IDs."""
        raw = request.args.get("photo_ids", "")
        if not raw:
            return json_error("photo_ids parameter is required")
        try:
            photo_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return json_error("photo_ids must be comma-separated integers")

        db = _get_db()
        subs = db.get_inat_submissions(photo_ids)
        # Convert keys to strings for JSON
        return jsonify({str(k): v for k, v in subs.items()})

    @app.route("/api/system/info")
    def api_system_info():
        """Return system information: ONNX Runtime, Python, hardware."""
        import platform

        info = {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "device": "CPU",
            "device_detail": "No GPU acceleration",
            "onnxruntime_version": None,
            "onnxruntime_providers": [],
        }
        try:
            import onnxruntime as ort

            info["onnxruntime_version"] = ort.__version__
            available = ort.get_available_providers()
            info["onnxruntime_providers"] = available

            if "CoreMLExecutionProvider" in available:
                info["device"] = "CoreML"
                info["device_detail"] = "Apple CoreML acceleration"
            elif "CUDAExecutionProvider" in available:
                info["device"] = "CUDA"
                info["device_detail"] = "NVIDIA CUDA acceleration"
            else:
                info["device"] = "CPU"
                info["device_detail"] = "GPU not available — using CPU"
        except ImportError:
            info["device_detail"] = "onnxruntime not installed"

        # MegaDetector status — just check for the ONNX file
        try:
            from detector import MEGADETECTOR_ONNX_PATH

            info["megadetector"] = "installed"
            info["megadetector_detail"] = "MegaDetector V6 (YOLOv9-c) — subject detection for crop-based classification"

            if os.path.isfile(MEGADETECTOR_ONNX_PATH):
                size_mb = round(os.path.getsize(MEGADETECTOR_ONNX_PATH) / 1024 / 1024, 1)
                info["megadetector_weights"] = "downloaded"
                info["megadetector_weights_path"] = MEGADETECTOR_ONNX_PATH
                info["megadetector_weights_size"] = f"{size_mb} MB"
            else:
                info["megadetector_weights"] = "not downloaded"
                info["megadetector_weights_path"] = None
                info["megadetector_weights_size"] = None
        except ImportError:
            info["megadetector"] = "unavailable"
            info["megadetector_detail"] = "detector module not available"

        return jsonify(info)

    @app.route("/api/megadetector/download", methods=["POST"])
    def api_megadetector_download():
        """Download MegaDetector ONNX model as a background job."""
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from detector import MEGADETECTOR_ONNX_DIR, MEGADETECTOR_ONNX_PATH
            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO

            os.makedirs(MEGADETECTOR_ONNX_DIR, exist_ok=True)

            runner.push_event(job["id"], "progress", {
                "phase": "Downloading MegaDetector ONNX model...",
                "current": 0, "total": 1,
            })

            import shutil

            cached_path = hf_hub_download(
                repo_id=ONNX_REPO,
                filename="model.onnx",
                subfolder="megadetector-v6",
            )

            dest = MEGADETECTOR_ONNX_PATH
            if cached_path != dest:
                shutil.copy2(cached_path, dest)

            if not os.path.isfile(dest):
                raise RuntimeError("Download completed but ONNX file not found")

            size_mb = round(os.path.getsize(dest) / 1024 / 1024, 1)
            return {"status": "downloaded", "size": f"{size_mb} MB", "path": dest}

        job_id = runner.start("download-megadetector", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/megadetector/delete", methods=["POST"])
    def api_megadetector_delete():
        """Delete MegaDetector ONNX model from disk."""
        import shutil

        import detector
        from detector import MEGADETECTOR_ONNX_DIR

        removed = []
        if os.path.isdir(MEGADETECTOR_ONNX_DIR):
            shutil.rmtree(MEGADETECTOR_ONNX_DIR)
            removed.append(MEGADETECTOR_ONNX_DIR)

        # Clear the cached singleton so it reloads next time
        detector._session = None

        return jsonify({"deleted": removed, "count": len(removed)})

    @app.route("/api/models/pipeline")
    def api_models_pipeline():
        """Return download status of all pipeline models (MegaDetector, SAM2, DINOv2)."""
        models_dir = os.path.expanduser("~/.vireo/models")
        models = []

        # MegaDetector — check for ONNX model in ~/.vireo/models/megadetector-v6/
        md_dir = os.path.join(models_dir, "megadetector-v6")
        md_onnx = os.path.join(md_dir, "model.onnx")
        md_status = "not downloaded"
        md_size = None
        if os.path.isfile(md_onnx):
            md_size = round(os.path.getsize(md_onnx) / 1024 / 1024, 1)
            md_status = "downloaded"
        models.append({
            "id": "megadetector-v6",
            "name": "MegaDetector V6",
            "role": "Detection",
            "description": "YOLOv9-c animal detector (ONNX)",
            "size_estimate": "~50 MB",
            "status": md_status,
            "size": f"{md_size} MB" if md_size else None,
        })

        # SAM2 variants — check for ONNX models in ~/.vireo/models/sam2-{variant}/
        sam2_variants = [
            ("sam2-tiny", "SAM2 Tiny", "~40 MB"),
            ("sam2-small", "SAM2 Small", "~150 MB"),
            ("sam2-base-plus", "SAM2 Base+", "~320 MB"),
            ("sam2-large", "SAM2 Large", "~900 MB"),
        ]
        for variant_id, name, size_est in sam2_variants:
            variant_dir = os.path.join(models_dir, variant_id)
            encoder_path = os.path.join(variant_dir, "image_encoder.onnx")
            decoder_path = os.path.join(variant_dir, "mask_decoder.onnx")
            status = "not downloaded"
            size = None
            if os.path.isfile(encoder_path) and os.path.isfile(decoder_path):
                total_size = os.path.getsize(encoder_path) + os.path.getsize(decoder_path)
                size = round(total_size / 1024 / 1024, 1)
                status = "downloaded"
            elif os.path.isfile(encoder_path) or os.path.isfile(decoder_path):
                status = "incomplete"
            models.append({
                "id": variant_id,
                "name": name,
                "role": "Segmentation",
                "description": f"SAM2 mask generation ({variant_id}, ONNX)",
                "size_estimate": size_est,
                "status": status,
                "size": f"{size} MB" if size else None,
            })

        # DINOv2 variants — check for ONNX models in ~/.vireo/models/dinov2-{variant}/
        dinov2_variants = [
            ("vit-s14", "DINOv2 ViT-S/14", "384-dim", "~85 MB"),
            ("vit-b14", "DINOv2 ViT-B/14", "768-dim", "~350 MB"),
            ("vit-l14", "DINOv2 ViT-L/14", "1024-dim", "~1.2 GB"),
        ]
        for variant_id, name, dims, size_est in dinov2_variants:
            variant_dir = os.path.join(models_dir, f"dinov2-{variant_id}")
            model_path = os.path.join(variant_dir, "model.onnx")
            status = "not downloaded"
            size = None
            if os.path.isfile(model_path):
                size = round(os.path.getsize(model_path) / 1024 / 1024, 1)
                status = "downloaded"
            models.append({
                "id": variant_id,
                "name": name,
                "role": "Embeddings",
                "description": f"{dims} embeddings for grouping (ONNX)",
                "size_estimate": size_est,
                "status": status,
                "size": f"{size} MB" if size else None,
            })

        return jsonify({"models": models})

    @app.route("/api/models/pipeline/download", methods=["POST"])
    def api_models_pipeline_download():
        """Download a pipeline model (ONNX) by ID from jss367/vireo-onnx-models."""
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        # Map each pipeline model ID to its HF subfolder and required files
        PIPELINE_MODELS = {
            "megadetector-v6": {
                "subfolder": "megadetector-v6",
                "files": ["model.onnx"],
            },
            "sam2-tiny": {
                "subfolder": "sam2-tiny",
                "files": ["image_encoder.onnx", "mask_decoder.onnx"],
            },
            "sam2-small": {
                "subfolder": "sam2-small",
                "files": ["image_encoder.onnx", "mask_decoder.onnx"],
            },
            "sam2-base-plus": {
                "subfolder": "sam2-base-plus",
                "files": ["image_encoder.onnx", "mask_decoder.onnx"],
            },
            "sam2-large": {
                "subfolder": "sam2-large",
                "files": ["image_encoder.onnx", "mask_decoder.onnx"],
            },
            "vit-s14": {
                "subfolder": "dinov2-vit-s14",
                "files": ["model.onnx"],
            },
            "vit-b14": {
                "subfolder": "dinov2-vit-b14",
                "files": ["model.onnx"],
            },
            "vit-l14": {
                "subfolder": "dinov2-vit-l14",
                "files": ["model.onnx"],
            },
        }

        if model_id not in PIPELINE_MODELS:
            return json_error(f"Unknown pipeline model: {model_id}")

        def work(job):
            import shutil

            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO

            spec = PIPELINE_MODELS[model_id]
            subfolder = spec["subfolder"]
            files = spec["files"]
            model_dir = os.path.join(os.path.expanduser("~/.vireo/models"), subfolder)
            os.makedirs(model_dir, exist_ok=True)

            total = len(files)
            for fi, filename in enumerate(files):
                runner.push_event(job["id"], "progress", {
                    "phase": f"Downloading {fi + 1}/{total}: {filename}...",
                    "current": fi,
                    "total": total,
                })

                MAX_RETRIES = 3
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        cached = hf_hub_download(
                            repo_id=ONNX_REPO,
                            filename=filename,
                            subfolder=subfolder,
                        )
                        dest = os.path.join(model_dir, filename)
                        if cached != dest:
                            shutil.copy2(cached, dest)
                        break
                    except Exception as exc:
                        if attempt == MAX_RETRIES:
                            raise
                        wait = 2 ** attempt
                        import time
                        log.warning(
                            "Download attempt %d/%d for %s/%s failed (%s), "
                            "retrying in %ds...",
                            attempt, MAX_RETRIES, subfolder, filename, exc, wait,
                        )
                        runner.push_event(job["id"], "progress", {
                            "phase": f"Retrying {filename} in {wait}s "
                                     f"(attempt {attempt}/{MAX_RETRIES})...",
                            "current": fi, "total": total,
                        })
                        time.sleep(wait)

            runner.push_event(job["id"], "progress", {
                "phase": "Download complete", "current": total, "total": total,
            })
            return {"status": "downloaded", "model_id": model_id}

        job_id = runner.start(f"download-{model_id}", work, config={"model_id": model_id}, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/models/pipeline/delete", methods=["POST"])
    def api_models_pipeline_delete():
        """Delete a pipeline model's ONNX files from ~/.vireo/models/."""
        import shutil

        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")

        models_dir = os.path.expanduser("~/.vireo/models")
        removed = []

        if model_id == "megadetector-v6":
            model_dir = os.path.join(models_dir, "megadetector-v6")
            if os.path.isdir(model_dir):
                shutil.rmtree(model_dir)
                removed.append(model_dir)
            import detector
            detector._session = None

        elif model_id.startswith("sam2-"):
            model_dir = os.path.join(models_dir, model_id)
            if os.path.isdir(model_dir):
                shutil.rmtree(model_dir)
                removed.append(model_dir)
            # Clear singleton
            import masking
            masking._encoder_session = None
            masking._decoder_session = None
            masking._sam2_variant_loaded = None

        elif model_id.startswith("vit-"):
            model_dir = os.path.join(models_dir, f"dinov2-{model_id}")
            if os.path.isdir(model_dir):
                shutil.rmtree(model_dir)
                removed.append(model_dir)
            # Clear singleton
            import dino_embed as dinov2_mod
            dinov2_mod._session = None
            dinov2_mod._variant_loaded = None

        return jsonify({"deleted": removed, "count": len(removed), "model_id": model_id})

    @app.route("/api/scan/status")
    def api_scan_status():
        db = _get_db()

        # DB file size
        db_size = 0
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)

        # Thumbnail cache size
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        thumb_size = 0
        if os.path.isdir(thumb_dir):
            for f in os.listdir(thumb_dir):
                fp = os.path.join(thumb_dir, f)
                if os.path.isfile(fp):
                    thumb_size += os.path.getsize(fp)

        return jsonify(
            {
                "photo_count": db.count_photos(),
                "folder_count": db.count_folders(),
                "keyword_count": db.count_keywords(),
                "pending_changes": db.count_pending_changes(),
                "db_size": db_size,
                "thumb_cache_size": thumb_size,
            }
        )

    # -- Job API routes --

    @app.route("/api/jobs/scan", methods=["POST"])
    def api_job_scan():
        body = request.get_json(silent=True) or {}
        root = body.get("root", "")
        incremental = body.get("incremental", False)
        if not root:
            return json_error("root path required")
        if not os.path.isdir(root):
            return json_error(f"directory not found: {root}")

        # Remember this scan root (skip temp directories from tests)
        import tempfile

        import config as cfg

        tmp_prefix = os.path.realpath(tempfile.gettempdir())
        if not os.path.realpath(root).startswith(tmp_prefix):
            user_cfg = cfg.load()
            roots = user_cfg.get("scan_roots", [])
            if root not in roots:
                roots.insert(0, root)
                user_cfg["scan_roots"] = roots
                cfg.save(user_cfg)

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from scanner import scan as do_scan

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            # Check folder health before scanning to prevent duplicate imports
            thread_db.check_folder_health()

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.update_step(job["id"], "scan",
                                   progress={"current": current, "total": total})
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": job["progress"].get("current_file", ""),
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Scanning photos",
                    },
                )

            job["_start_time"] = time.time()
            runner.set_steps(job["id"], [
                {"id": "scan", "label": "Scan photos"},
                {"id": "thumbnails", "label": "Generate thumbnails"},
            ])
            runner.update_step(job["id"], "scan", status="running")
            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            def status_cb(message):
                runner.update_step(job["id"], "scan", current_file=message)
                runner.push_event(job["id"], "progress", {
                    "phase": message,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": message,
                    "rate": 0,
                })

            do_scan(
                root, thread_db, progress_callback=progress_cb, incremental=incremental,
                extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                status_callback=status_cb,
            )
            photo_count = job["progress"].get("total", 0)
            runner.update_step(job["id"], "scan", status="completed",
                               summary=f"{photo_count} photos")
            runner.update_step(job["id"], "thumbnails", status="running")

            # Auto-generate thumbnails for new photos only
            from thumbnails import generate_all

            log.info("Generating thumbnails...")
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": "Checking for new thumbnails...",
                    "rate": 0,
                    "phase": "Generating thumbnails",
                },
            )

            def thumb_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": "",
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Generating thumbnails",
                    },
                )

            thumb_result = generate_all(
                thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=thumb_cb
            )
            from thumbnails import format_summary as thumb_summary
            runner.update_step(job["id"], "thumbnails", status="completed",
                               summary=thumb_summary(thumb_result))

            return {"photos_indexed": photo_count, "thumbnails": thumb_result}

        job_id = runner.start(
            "scan", work, config={"root": root, "incremental": incremental},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/thumbnails", methods=["POST"])
    def api_job_thumbnails():
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from thumbnails import generate_all

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                    },
                )

            job["_start_time"] = time.time()
            return generate_all(
                thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=progress_cb
            )

        job_id = runner.start("thumbnails", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/previews", methods=["POST"])
    def api_job_previews():
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            import config as cfg
            from image_loader import load_image

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            max_size = cfg.get("preview_max_size") or 1920
            if max_size == 0:
                max_size = None  # Full resolution
            preview_quality = cfg.load().get("preview_quality", 90)
            preview_dir = os.path.join(
                os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
            )
            os.makedirs(preview_dir, exist_ok=True)

            if collection_id:
                photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            else:
                photos = thread_db.get_photos(per_page=999999)

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            generated = 0
            skipped = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                cache_path = os.path.join(preview_dir, f'{photo["id"]}.jpg')
                if os.path.exists(cache_path):
                    skipped += 1
                else:
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])
                    img = load_image(image_path, max_size=max_size)
                    if img:
                        img.save(cache_path, format="JPEG", quality=preview_quality)
                        generated += 1

                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"],
                        "rate": round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Generating previews",
                    },
                )

            return {"generated": generated, "skipped": skipped, "total": total}

        job_id = runner.start("previews", work, config={"collection_id": collection_id},
                               workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/ingest", methods=["POST"])
    def api_job_ingest():
        body = request.get_json(silent=True) or {}
        source = body.get("source", "")
        destination = body.get("destination", "")
        file_types = body.get("file_types", "both")
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        skip_duplicates = body.get("skip_duplicates", True)

        if not source or not destination:
            return json_error("source and destination are required")
        if not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from ingest import ingest as do_ingest

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            job["_start_time"] = time.time()

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Importing photos",
                    },
                )

            result = do_ingest(
                source_dir=source,
                destination_dir=destination,
                db=thread_db,
                file_types=file_types,
                folder_template=folder_template,
                skip_duplicates=skip_duplicates,
                progress_callback=progress_cb,
            )
            return result

        job_id = runner.start(
            "ingest",
            work,
            config={
                "source": source,
                "destination": destination,
                "file_types": file_types,
                "folder_template": folder_template,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    # -- Move job routes --

    @app.route("/api/jobs/move-photos", methods=["POST"])
    def api_job_move_photos():
        """Move selected photos to a destination directory."""
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        destination = body.get("destination", "")
        rule_id = body.get("rule_id")

        if not photo_ids:
            return json_error("photo_ids required")
        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from move import move_photos

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            job["_start_time"] = time.time()
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "rate": round(
                        current / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "phase": "Moving photos",
                })

            result = move_photos(
                db=thread_db,
                photo_ids=photo_ids,
                destination=destination,
                progress_cb=progress_cb,
            )

            if rule_id:
                thread_db.touch_move_rule(rule_id)

            return result

        job_id = runner.start(
            "move-photos", work,
            config={"photo_ids": photo_ids, "destination": destination},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/move-folder", methods=["POST"])
    def api_job_move_folder():
        """Move an entire folder to a destination."""
        body = request.get_json(silent=True) or {}
        folder_id = body.get("folder_id")
        destination = body.get("destination", "")

        if not folder_id:
            return json_error("folder_id required")
        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from move import move_folder

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            job["_start_time"] = time.time()

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "phase": "Moving folder",
                })

            return move_folder(
                db=thread_db,
                folder_id=folder_id,
                destination=destination,
                progress_cb=progress_cb,
            )

        job_id = runner.start(
            "move-folder", work,
            config={"folder_id": folder_id, "destination": destination},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/import-full", methods=["POST"])
    def api_job_import_full():
        """Full-chain import: copy files -> scan -> create collection."""
        body = request.get_json(silent=True) or {}
        source = body.get("source", "")
        destination = body.get("destination", "")
        file_types = body.get("file_types", "both")
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        skip_duplicates = body.get("skip_duplicates", True)
        copy = body.get("copy", True)
        exclude_paths = set(body.get("exclude_paths", []))

        if not source:
            return json_error("source is required")
        if not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")
        if copy:
            if not destination:
                return json_error("source and destination are required")
            if not os.path.isabs(destination):
                return json_error("destination must be an absolute path")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from scanner import scan as do_scan
            from thumbnails import generate_all

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            # Check folder health before scanning to prevent duplicate imports
            thread_db.check_folder_health()
            job["_start_time"] = time.time()

            scan_target = str(Path(source))  # normalize (strips trailing slash)

            # Define steps based on whether we're copying
            steps = []
            if copy:
                steps.append({"id": "ingest", "label": "Import photos"})
            steps.extend([
                {"id": "scan", "label": "Scan photos"},
                {"id": "thumbnails", "label": "Generate thumbnails"},
                {"id": "collection", "label": "Create collection"},
            ])
            runner.set_steps(job["id"], steps)

            if copy:
                from ingest import ingest as do_ingest

                # Phase 1: Copy files
                runner.update_step(job["id"], "ingest", status="running")

                def ingest_cb(current, total, filename):
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    job["progress"]["current_file"] = filename
                    runner.push_event(job["id"], "progress", {
                        "current": current, "total": total,
                        "current_file": filename,
                        "phase": "Importing photos",
                    })

                ingest_result = do_ingest(
                    source_dir=source,
                    destination_dir=destination,
                    db=thread_db,
                    file_types=file_types,
                    folder_template=folder_template,
                    skip_duplicates=skip_duplicates,
                    progress_callback=ingest_cb,
                    skip_paths=exclude_paths or None,
                )
                copied_paths = ingest_result.get("copied_paths", [])
                scan_target = destination
                runner.update_step(job["id"], "ingest", status="completed",
                                   summary=f"{ingest_result.get('copied', 0)} copied")

            # Phase 2: Scan to index into DB
            runner.update_step(job["id"], "scan", status="running")

            def scan_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": "",
                    "phase": "Scanning photos",
                })

            do_scan(scan_target, thread_db, progress_callback=scan_cb, skip_paths=exclude_paths or None)
            scan_count = job["progress"].get("total", 0)
            runner.update_step(job["id"], "scan", status="completed",
                               summary=f"{scan_count} photos")

            # Phase 3: Generate thumbnails
            runner.update_step(job["id"], "thumbnails", status="running")
            runner.push_event(job["id"], "progress", {
                "current": 0, "total": 0,
                "current_file": "Checking for new thumbnails...",
                "phase": "Generating thumbnails",
            })

            def thumb_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": "",
                    "phase": "Generating thumbnails",
                })

            thumb_result = generate_all(
                thread_db, app.config["THUMB_CACHE_DIR"],
                progress_callback=thumb_cb,
            )
            from thumbnails import format_summary as thumb_summary
            runner.update_step(job["id"], "thumbnails", status="completed",
                               summary=thumb_summary(thumb_result))

            # Phase 4: Create collection
            runner.update_step(job["id"], "collection", status="running")
            photo_ids = []
            if copy:
                # Collection from copied files (existing logic)
                if copied_paths:
                    thread_db.conn.execute(
                        "CREATE TEMP TABLE IF NOT EXISTS _imported_paths (dirpath TEXT, fname TEXT)"
                    )
                    thread_db.conn.execute("DELETE FROM _imported_paths")
                    thread_db.conn.executemany(
                        "INSERT INTO _imported_paths (dirpath, fname) VALUES (?, ?)",
                        [(os.path.dirname(p), os.path.basename(p)) for p in copied_paths],
                    )
                    rows = thread_db.conn.execute(
                        """SELECT p.id FROM photos p
                           JOIN folders f ON p.folder_id = f.id
                           JOIN _imported_paths ip ON f.path = ip.dirpath
                                                   AND p.filename = ip.fname"""
                    ).fetchall()
                    photo_ids = [r["id"] for r in rows]
                    thread_db.conn.execute("DROP TABLE IF EXISTS _imported_paths")
            else:
                # Collection from all photos in the scanned folder
                rows = thread_db.conn.execute(
                    """SELECT p.id FROM photos p
                       JOIN folders f ON p.folder_id = f.id
                       WHERE f.path = ? OR f.path LIKE ?""",
                    (scan_target, scan_target.rstrip("/") + "/%"),
                ).fetchall()
                photo_ids = [r["id"] for r in rows]

            collection_id = None
            collection_name = None
            if photo_ids:
                from datetime import datetime as dt
                collection_name = "Import " + dt.now().strftime("%Y-%m-%d %H:%M")
                collection_id = thread_db.add_collection(
                    collection_name,
                    json.dumps([{"field": "photo_ids", "value": photo_ids}]),
                )

            col_summary = collection_name if collection_name else "no photos"
            runner.update_step(job["id"], "collection", status="completed",
                               summary=col_summary)

            result = {
                "photos_indexed": len(photo_ids),
                "collection_id": collection_id,
                "collection_name": collection_name,
            }
            if copy:
                result["copied"] = ingest_result.get("copied", 0)
                result["skipped_duplicate"] = ingest_result.get("skipped_duplicate", 0)
                result["failed"] = ingest_result.get("failed", 0)
                result["total"] = ingest_result.get("total", 0)

            return result

        job_id = runner.start(
            "import-full", work,
            config={"source": source, "destination": destination, "copy": copy, "file_types": file_types},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/import", methods=["POST"])
    def api_job_import():
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        strategy = body.get("strategy", "merge_all")
        write_xmp = body.get("write_xmp", False)
        if not catalogs:
            return json_error("catalogs required")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from importer import execute_import

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                    },
                )

            return execute_import(
                catalogs,
                thread_db,
                write_xmp=write_xmp,
                strategy=strategy,
                progress_callback=progress_cb,
            )

        job_id = runner.start(
            "import", work, config={"catalogs": catalogs, "strategy": strategy},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/sync", methods=["POST"])
    def api_job_sync():
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from sync import sync_to_xmp

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                    },
                )

            return sync_to_xmp(thread_db, progress_callback=progress_cb)

        job_id = runner.start("sync", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/sharpness", methods=["POST"])
    def api_job_sharpness():
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from sharpness import score_collection_photos

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            job["_start_time"] = time.time()

            runner.set_steps(job["id"], [
                {"id": "score", "label": "Score sharpness"},
                {"id": "save", "label": "Save results & auto-flag"},
            ])
            runner.update_step(job["id"], "score", status="running")

            def progress_cb(current, total, msg):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = msg
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": msg,
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Scoring sharpness",
                    },
                )

            result = score_collection_photos(
                thread_db,
                collection_id,
                progress_callback=progress_cb,
            )
            runner.update_step(job["id"], "score", status="completed",
                               summary=f"{len(result['results'])} scored")

            # Save scores to database
            runner.update_step(job["id"], "save", status="running")
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": len(result["results"]),
                    "current_file": "Saving scores to database...",
                    "rate": 0,
                    "phase": "Saving results",
                },
            )
            for r in result["results"]:
                thread_db.update_photo_sharpness(r["photo_id"], r["sharpness"])

            # Auto-flag: flag best in each group, suggest reject for worst
            best_count = 0
            for r in result["results"]:
                if r["group_size"] > 1 and r["is_best"]:
                    thread_db.update_photo_flag(r["photo_id"], "flagged")
                    best_count += 1

            result["auto_flagged"] = best_count
            runner.update_step(job["id"], "save", status="completed",
                               summary=f"{best_count} flagged")
            # Don't return the full results list (could be huge)
            del result["results"]
            return result

        job_id = runner.start(
            "sharpness",
            work,
            config={
                "collection_id": collection_id,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/classify", methods=["POST"])
    def api_job_classify():
        import config as cfg
        from classify_job import ClassifyParams, run_classify_job

        user_cfg = _get_db().get_effective_config(cfg.load())
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        if not collection_id:
            return json_error("collection_id required")

        params = ClassifyParams(
            collection_id=collection_id,
            labels_file=body.get("labels_file"),
            labels_files=body.get("labels_files"),
            model_id=body.get("model_id"),
            model_name=body.get("model_name"),
            grouping_window=body.get(
                "grouping_window", user_cfg["grouping_window_seconds"]
            ),
            similarity_threshold=body.get(
                "similarity_threshold", user_cfg.get("similarity_threshold", 0.85)
            ),
            reclassify=body.get("reclassify", False),
        )

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            return run_classify_job(job, runner, db_path, active_ws, params)

        job_id = runner.start(
            "classify",
            work,
            config={
                "collection_id": collection_id,
                "model_name": params.model_name,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs")
    def api_jobs_list():
        runner = app._job_runner
        db = _get_db()
        active = runner.list_jobs()
        history = runner.get_history(db, limit=10)
        ws_rows = db.get_workspaces()
        ws_names = {w["id"]: w["name"] for w in ws_rows}
        return jsonify({
            "active": active,
            "history": history,
            "active_workspace_id": db._active_workspace_id,
            "workspace_names": ws_names,
        })

    @app.route("/api/jobs/<job_id>")
    def api_job_status(job_id):
        job = app._job_runner.get(job_id)
        if not job:
            return json_error("job not found", 404)
        return jsonify(job)

    @app.route("/api/jobs/<job_id>/stream")
    def api_job_stream(job_id):
        """SSE stream of job progress events."""
        runner = app._job_runner
        job = runner.get(job_id)
        if not job:
            return json_error("job not found", 404)

        q = runner.subscribe(job_id)

        def generate():
            try:
                while True:
                    try:
                        event = q.get(timeout=1)
                        yield f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                        if event["type"] == "complete":
                            break
                    except queue.Empty:
                        # Send keepalive
                        yield ": keepalive\n\n"
                        # Check if job is done (in case we missed the complete event)
                        j = runner.get(job_id)
                        if j is None:
                            # Job was pruned from finished jobs dict; true terminal
                            # status is unknown (could have been completed, failed, or
                            # cancelled before pruning).  Emit "expired" so callers do
                            # not incorrectly execute success-only code paths.
                            yield f"event: complete\ndata: {json.dumps({'status': 'expired', 'result': None, 'errors': ['job expired from server memory before stream could read final status']})}\n\n"
                            break
                        if j["status"] in ("completed", "failed", "cancelled"):
                            yield f"event: complete\ndata: {json.dumps({'status': j['status'], 'result': j['result'], 'errors': j['errors']})}\n\n"
                            break
            finally:
                runner.unsubscribe(job_id, q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # -- Global log stream --

    @app.route("/api/logs/stream")
    def api_log_stream():
        """SSE stream of all server log output.

        Auto-closes after 6s of inactivity to prevent stale connections
        from exhausting Flask's thread pool during page navigation.
        The browser's EventSource will auto-reconnect.
        """
        broadcaster = app._log_broadcaster
        q = broadcaster.subscribe()

        def generate():
            idle_count = 0
            try:
                while True:
                    try:
                        record = q.get(timeout=2)
                        yield f"event: log\ndata: {json.dumps(record)}\n\n"
                        idle_count = 0
                    except queue.Empty:
                        idle_count += 1
                        yield ": keepalive\n\n"
                        # Close after ~6s idle to free the thread
                        if idle_count >= 3:
                            return
            except GeneratorExit:
                pass
            finally:
                broadcaster.unsubscribe(q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.route("/api/logs/recent")
    def api_logs_recent():
        count = request.args.get("count", 100, type=int)
        return jsonify(app._log_broadcaster.get_recent(count))

    @app.route("/api/jobs/history")
    def api_job_history():
        db = _get_db()
        limit = request.args.get("limit", 10, type=int)
        return jsonify(app._job_runner.get_history(db, limit=limit))

    # -- Image serving --

    @app.route("/favicon.ico")
    def favicon():
        return send_from_directory(
            os.path.join(os.path.dirname(__file__), "static"),
            "favicon.png",
            mimetype="image/png",
        )

    @app.route("/thumbnails/<filename>")
    def serve_thumbnail(filename):
        def _send_cached(directory, fname):
            resp = make_response(send_from_directory(directory, fname))
            resp.cache_control.public = True
            resp.cache_control.max_age = 24 * 60 * 60  # 1 day
            return resp

        thumb_path = os.path.join(app.config["THUMB_CACHE_DIR"], filename)
        if os.path.exists(thumb_path):
            return _send_cached(app.config["THUMB_CACHE_DIR"], filename)

        # Try to generate on the fly
        try:
            photo_id = int(filename.replace(".jpg", ""))
            db = _get_db()
            photo = db.conn.execute(
                "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
                (photo_id,),
            ).fetchone()
            if photo:
                from thumbnails import generate_thumbnail
                source = os.path.join(photo["path"], photo["filename"])
                result = generate_thumbnail(photo_id, source, app.config["THUMB_CACHE_DIR"])
                if result:
                    return _send_cached(app.config["THUMB_CACHE_DIR"], filename)
        except Exception:
            pass

        return "", 404

    @app.route("/api/species/<path:species_name>/clusters")
    def api_species_clusters(species_name):
        """Cluster photos of a species by visual similarity to find variants.

        Uses agglomerative clustering on BioCLIP embeddings to discover
        sub-groups (male/female, juvenile/adult, etc.).
        """
        import numpy as np

        db = _get_db()
        distance_threshold = request.args.get("threshold", 0.4, type=float)

        # Find all photos with this species prediction
        rows = db.conn.execute(
            """SELECT d.photo_id, p.embedding, p.filename, p.thumb_path,
                      pr.confidence, pr.taxonomy_order, pr.taxonomy_family
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               WHERE pr.species = ? AND d.workspace_id = ? AND p.embedding IS NOT NULL""",
            (species_name, db._active_workspace_id),
        ).fetchall()

        if len(rows) < 2:
            # Not enough photos to cluster
            photos = []
            for r in rows:
                photo = db.get_photo(r["photo_id"])
                if photo:
                    photos.append({"photo": dict(photo), "cluster": 0})
            return jsonify(
                {
                    "species": species_name,
                    "clusters": (
                        [{"id": 0, "label": "", "photos": photos}] if photos else []
                    ),
                    "total_photos": len(rows),
                }
            )

        # Load embeddings
        photo_data = []
        embeddings = []
        for r in rows:
            emb = np.frombuffer(r["embedding"], dtype=np.float32)
            embeddings.append(emb)
            photo = db.get_photo(r["photo_id"])
            if photo:
                photo_data.append(dict(photo))
            else:
                photo_data.append({"id": r["photo_id"], "filename": r["filename"]})

        emb_matrix = np.stack(embeddings)

        # Cosine distance matrix (1 - similarity)
        # Embeddings are normalized, so dot product = cosine similarity
        sim_matrix = emb_matrix @ emb_matrix.T
        dist_matrix = np.maximum(1.0 - sim_matrix, 0.0)
        np.fill_diagonal(dist_matrix, 0)

        # Agglomerative clustering
        from scipy.cluster.hierarchy import fcluster, linkage
        from scipy.spatial.distance import squareform

        condensed = squareform(dist_matrix, checks=False)
        Z = linkage(condensed, method="average")
        labels = fcluster(Z, t=distance_threshold, criterion="distance")

        # Group photos by cluster
        cluster_map = {}
        for i, label in enumerate(labels):
            cid = int(label) - 1
            if cid not in cluster_map:
                cluster_map[cid] = []
            cluster_map[cid].append(
                {
                    "photo": photo_data[i],
                    "cluster": cid,
                }
            )

        # Sort clusters by size (largest first)
        clusters = []
        for cid in sorted(cluster_map.keys(), key=lambda k: -len(cluster_map[k])):
            clusters.append(
                {
                    "id": cid,
                    "label": "",
                    "count": len(cluster_map[cid]),
                    "photos": cluster_map[cid],
                }
            )

        return jsonify(
            {
                "species": species_name,
                "clusters": clusters,
                "total_photos": len(rows),
                "num_clusters": len(clusters),
                "distance_threshold": distance_threshold,
            }
        )

    @app.route("/api/species/label-cluster", methods=["POST"])
    def api_label_cluster():
        """Apply a label (e.g. 'male', 'juvenile') to all photos in a cluster."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        label = body.get("label", "").strip()
        if not photo_ids or not label:
            return json_error("photo_ids and label required")

        kid = db.add_keyword(label)
        for pid in photo_ids:
            db.tag_photo(pid, kid)
            db.queue_change(pid, "keyword_add", label)

        items = [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)} for pid in photo_ids]
        db.record_edit('keyword_add', f'Labeled {len(photo_ids)} photos as "{label}"',
                       str(kid), items, is_batch=len(photo_ids) > 1)

        log.info("Labeled %d photos as '%s'", len(photo_ids), label)
        return jsonify({"ok": True, "updated": len(photo_ids), "keyword_id": kid})

    @app.route("/api/species/summary")
    def api_species_list():
        """List all species with prediction counts, for the variant explorer."""
        db = _get_db()
        rows = db.conn.execute(
            """SELECT pr.species, COUNT(DISTINCT d.photo_id) as photo_count,
                      pr.taxonomy_order, pr.taxonomy_family, pr.taxonomy_genus,
                      pr.scientific_name
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE pr.status != 'rejected' AND d.workspace_id = ?
               GROUP BY pr.species
               ORDER BY photo_count DESC""",
            (db._active_workspace_id,),
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    # -- Culling API --

    @app.route("/api/jobs/cull", methods=["POST"])
    def api_job_cull():
        """Run culling analysis as a background job."""
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        separate_file_types = body.get("separate_file_types", True)
        time_window = body.get("time_window", 60)
        phash_threshold = body.get("phash_threshold", 19)
        cross_bucket_merge = body.get("cross_bucket_merge", False)

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from culling import analyze_for_culling

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            def progress_cb(msg):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                        "phase": "Culling analysis",
                    },
                )

            progress_cb("Analyzing photos for culling...")

            result = analyze_for_culling(
                thread_db,
                collection_id=collection_id,
                separate_file_types=separate_file_types,
                time_window=time_window,
                phash_threshold=phash_threshold,
                cross_bucket_merge=cross_bucket_merge,
                progress_callback=progress_cb,
            )

            # Store culling results in a temporary cache for the UI
            import json as _json

            cache_path = os.path.join(
                os.path.dirname(db_path), f"culling_results_ws{active_ws}.json"
            )
            with open(cache_path, "w") as f:
                _json.dump(result, f)

            return {
                "total_photos": result["total_photos"],
                "suggested_keepers": result["suggested_keepers"],
                "suggested_rejects": result["suggested_rejects"],
                "species_count": len(result["species_groups"]),
            }

        job_id = runner.start(
            "cull", work, config={"collection_id": collection_id},
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/develop", methods=["POST"])
    def api_job_develop():
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        if not photo_ids:
            return json_error("photo_ids required")

        import config as cfg
        from develop import find_darktable

        darktable_bin = cfg.get("darktable_bin")
        binary = find_darktable(darktable_bin)
        if not binary:
            return json_error("darktable-cli not found. Configure the path in Settings.")

        style = body.get("style") or cfg.get("darktable_style") or ""
        output_format = body.get("output_format") or cfg.get("darktable_output_format") or "jpg"
        output_dir = body.get("output_dir") or cfg.get("darktable_output_dir") or ""
        width = body.get("width")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from develop import develop_photo, output_path_for_photo

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            photos = []
            for pid in photo_ids:
                p = thread_db.get_photo(pid)
                if p:
                    photos.append(p)

            if not photos:
                return {"developed": 0, "errors": 0, "total": 0}

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            developed = 0
            errors = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                folder_path = folders.get(photo["folder_id"], "")
                input_path = os.path.join(folder_path, photo["filename"])

                # Determine output directory: configured, or "developed" subfolder next to originals
                out_dir = output_dir if output_dir else os.path.join(folder_path, "developed")
                out_path = output_path_for_photo(photo["filename"], out_dir, output_format)

                result = develop_photo(
                    darktable_bin=binary,
                    input_path=input_path,
                    output_path=out_path,
                    style=style if style else None,
                    width=width,
                )

                if result["success"]:
                    developed += 1
                else:
                    errors += 1
                    job["errors"].append(f'{photo["filename"]}: {result["error"]}')
                    log.warning("Failed to develop %s: %s", photo["filename"], result["error"])

                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"],
                        "rate": round(
                            (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Developing photos",
                    },
                )

            return {"developed": developed, "errors": errors, "total": total}

        job_id = runner.start(
            "develop",
            work,
            config={
                "photo_ids": photo_ids,
                "style": style,
                "output_format": output_format,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    # -- Pipeline: SAM2 Mask Extraction --

    @app.route("/api/jobs/extract-masks", methods=["POST"])
    def api_job_extract_masks():
        """Run SAM2 mask extraction as a background job.

        Requires MegaDetector detections to already be computed (run classify first).
        For each photo with a detection but no mask, loads a working-resolution proxy,
        runs SAM2 to refine the bounding box into a pixel mask, and saves it.
        """
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        import config as cfg

        effective_cfg = _get_db().get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})
        sam2_variant = pipeline_cfg.get("sam2_variant", "sam2-small")
        dinov2_variant = pipeline_cfg.get("dinov2_variant", "vit-b14")
        proxy_longest_edge = pipeline_cfg.get("proxy_longest_edge", 1536)

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from dino_embed import embed_global, embed_subject, embedding_to_blob
            from masking import (
                crop_completeness,
                crop_subject,
                generate_mask,
                render_proxy,
                save_mask,
            )
            from quality import compute_all_quality_features

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            masks_dir = os.path.join(os.path.dirname(db_path), "masks")
            os.makedirs(masks_dir, exist_ok=True)

            # Get photos that have detections but no masks
            if collection_id:
                coll_photos = thread_db.get_collection_photos(
                    collection_id, per_page=999999
                )
                # Filter to photos with detections but no masks
                ws_id = thread_db._active_workspace_id
                photos = []
                for p in coll_photos:
                    if p["mask_path"]:
                        continue
                    det = thread_db.conn.execute(
                        """SELECT box_x, box_y, box_w, box_h, detector_confidence
                           FROM detections
                           WHERE photo_id = ? AND workspace_id = ?
                           ORDER BY detector_confidence DESC LIMIT 1""",
                        (p["id"], ws_id),
                    ).fetchone()
                    if det:
                        photos.append({
                            "id": p["id"],
                            "folder_id": p["folder_id"],
                            "filename": p["filename"],
                            "detection_box": json.dumps({
                                "x": det["box_x"], "y": det["box_y"],
                                "w": det["box_w"], "h": det["box_h"],
                            }),
                            "detection_conf": det["detector_confidence"],
                        })
            else:
                photos = thread_db.get_photos_missing_masks()

            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            masked = 0
            skipped = 0
            failed = 0
            job["_start_time"] = time.time()

            for i, photo in enumerate(photos):
                photo_id = photo["id"]
                folder_path = folders.get(photo["folder_id"], "")
                image_path = os.path.join(folder_path, photo["filename"])

                try:
                    # Load working-resolution proxy
                    proxy = render_proxy(image_path, longest_edge=proxy_longest_edge)
                    if proxy is None:
                        skipped += 1
                        continue

                    # Parse detection box
                    det_box = photo["detection_box"]
                    if isinstance(det_box, str):
                        det_box = json.loads(det_box)

                    # Generate mask via SAM2
                    mask = generate_mask(proxy, det_box, variant=sam2_variant)
                    if mask is None:
                        skipped += 1
                        continue

                    # Save mask PNG
                    mask_path = save_mask(mask, masks_dir, photo_id)

                    # Compute crop completeness + all quality features
                    completeness = crop_completeness(mask)
                    features = compute_all_quality_features(proxy, mask)

                    # Compute DINOv2 embeddings
                    subject_crop = crop_subject(proxy, mask, margin=0.15)
                    subj_emb_blob = None
                    global_emb_blob = None
                    if subject_crop is not None:
                        subj_emb = embed_subject(subject_crop, variant=dinov2_variant)
                        subj_emb_blob = embedding_to_blob(subj_emb)
                    global_emb = embed_global(proxy, variant=dinov2_variant)
                    global_emb_blob = embedding_to_blob(global_emb)

                    # Update DB with mask path, completeness, features, and embeddings
                    thread_db.update_photo_pipeline_features(
                        photo_id,
                        mask_path=mask_path,
                        crop_complete=completeness,
                        **features,
                    )
                    thread_db.update_photo_embeddings(
                        photo_id,
                        dino_subject_embedding=subj_emb_blob,
                        dino_global_embedding=global_emb_blob,
                    )
                    masked += 1

                except Exception:
                    failed += 1
                    log.warning(
                        "Mask extraction failed for photo %s", photo_id, exc_info=True
                    )
                    job["errors"].append(
                        f"Photo {photo_id}: mask extraction failed"
                    )

                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": i + 1,
                        "total": total,
                        "current_file": photo["filename"]
                        if hasattr(photo, "__getitem__") and "filename" in photo.keys()
                        else str(photo_id),
                        "rate": round(
                            (i + 1)
                            / max(time.time() - job["_start_time"], 0.01),
                            1,
                        ),
                        "phase": "Extracting features (SAM2 + DINOv2)",
                    },
                )

            return {"masked": masked, "skipped": skipped, "failed": failed, "total": total}

        job_id = runner.start(
            "extract-masks",
            work,
            config={
                "collection_id": collection_id,
                "sam2_variant": sam2_variant,
                "dinov2_variant": dinov2_variant,
                "proxy_longest_edge": proxy_longest_edge,
            },
            workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/regroup", methods=["POST"])
    def api_job_regroup():
        """Run pipeline stages 2-6 (grouping + scoring + triage) from cached features.

        This is fast (seconds) — no model inference, just math on stored features.
        Requires extract-masks to have been run first.
        """
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        import config as cfg

        effective_cfg = _get_db().get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from pipeline import (
                load_photo_features,
                run_full_pipeline,
                save_results,
            )

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)

            runner.set_steps(job["id"], [
                {"id": "load", "label": "Load features"},
                {"id": "group", "label": "Group encounters & bursts"},
                {"id": "save", "label": "Save results"},
            ])

            runner.update_step(job["id"], "load", status="running")
            runner.push_event(
                job["id"],
                "progress",
                {"phase": "Loading features from database", "current": 0, "total": 3},
            )

            photos = load_photo_features(thread_db, collection_id=collection_id, config=effective_cfg)
            if not photos:
                runner.update_step(job["id"], "load", status="failed",
                                   error="No photos with pipeline features found")
                return {"error": "No photos with pipeline features found. Run extract-masks first."}
            runner.update_step(job["id"], "load", status="completed",
                               summary=f"{len(photos)} photos")

            runner.update_step(job["id"], "group", status="running")
            runner.push_event(
                job["id"],
                "progress",
                {"phase": "Grouping encounters and bursts", "current": 1, "total": 3},
            )

            results = run_full_pipeline(photos, config=pipeline_cfg)
            summary = results.get("summary", {})
            runner.update_step(job["id"], "group", status="completed",
                               summary=f"{summary.get('encounters', 0)} encounters")

            runner.update_step(job["id"], "save", status="running")
            runner.push_event(
                job["id"],
                "progress",
                {"phase": "Saving results", "current": 2, "total": 3},
            )

            cache_dir = os.path.dirname(db_path)
            save_results(results, cache_dir, active_ws)
            runner.update_step(job["id"], "save", status="completed")

            return results["summary"]

        job_id = runner.start("regroup", work, config={"pipeline": pipeline_cfg}, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/pipeline", methods=["POST"])
    def api_job_pipeline():
        """Streaming pipeline: scan -> thumbnails -> classify -> extract-masks -> regroup.

        Overlaps I/O stages and interleaves detection with classification.
        Provide either 'source' (for import+scan) or 'collection_id' (skip scan).
        """
        from pipeline_job import PipelineParams, run_pipeline_job

        body = request.get_json(silent=True) or {}
        source = body.get("source")
        sources = body.get("sources")
        collection_id = body.get("collection_id")

        if not source and not sources and not collection_id:
            return json_error("source, sources, or collection_id required")

        # Validate all source directories exist
        if sources:
            for s in sources:
                if not os.path.isdir(s):
                    return json_error(f"source directory not found: {s}")
        elif source and not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")

        destination = body.get("destination")
        if destination and not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        params = PipelineParams(
            collection_id=collection_id,
            source=source,
            sources=sources,
            destination=destination,
            file_types=body.get("file_types", "both"),
            folder_template=body.get("folder_template", "%Y/%Y-%m-%d"),
            skip_duplicates=body.get("skip_duplicates", True),
            labels_file=body.get("labels_file"),
            labels_files=body.get("labels_files"),
            model_id=body.get("model_id"),
            reclassify=body.get("reclassify", False),
            skip_classify=body.get("skip_classify", False),
            download_taxonomy=body.get("download_taxonomy", True),
            skip_extract_masks=body.get("skip_extract_masks", False),
            skip_regroup=body.get("skip_regroup", False),
            preview_max_size=body.get("preview_max_size", 1920),
            exclude_paths=set(body.get("exclude_paths", [])) or None,
            recursive=body.get("recursive", True),
        )

        # Auto-skip classify stages if no model is available
        model_warning = None
        if not params.skip_classify:
            from models import get_active_model, get_models

            _model = None
            if params.model_id:
                _all = get_models()
                _model = next((m for m in _all if m["id"] == params.model_id and m["downloaded"]), None)
            if not _model:
                _model = get_active_model()
            if not _model:
                params.skip_classify = True
                params.skip_extract_masks = True
                params.skip_regroup = True
                model_warning = "No model available \u2014 classification was skipped. Download a model in Settings to enable species identification."

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            return run_pipeline_job(job, runner, db_path, active_ws, params)

        job_id = runner.start(
            "pipeline", work,
            config={
                "source": source,
                "sources": sources,
                "collection_id": collection_id,
                "skip_classify": params.skip_classify,
                "skip_extract_masks": params.skip_extract_masks,
                "skip_regroup": params.skip_regroup,
            },
            workspace_id=active_ws,
        )
        result = {"job_id": job_id}
        if model_warning:
            result["model_warning"] = model_warning
        return jsonify(result)

    @app.route("/api/encounters/species", methods=["POST"])
    def api_encounter_species():
        """Confirm species for all photos in an encounter.

        Expects JSON: {"species": "Blue Jay", "photo_ids": [1, 2, 3]}
        Creates species keyword, tags photos, queues pending changes.
        """
        db = _get_db()
        body = request.get_json(silent=True) or {}
        species = body.get("species", "").strip()
        photo_ids = body.get("photo_ids", [])

        if not species:
            return json_error("species is required")
        if not photo_ids:
            return json_error("photo_ids is required")

        # Validate all photo_ids exist before mutating
        placeholders = ",".join("?" for _ in photo_ids)
        found = db.conn.execute(
            f"SELECT id FROM photos WHERE id IN ({placeholders})", photo_ids
        ).fetchall()
        found_ids = {r["id"] for r in found}
        missing = [pid for pid in photo_ids if pid not in found_ids]
        if missing:
            return json_error(f"Unknown photo_ids: {missing}")

        # Create or find the species keyword (commits on its own)
        kid = db.add_keyword(species, is_species=True)

        # Tag all photos and queue pending changes in a single transaction
        ws_id = db._ws_id()
        try:
            for pid in photo_ids:
                db.conn.execute(
                    "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
                    (pid, kid),
                )
                existing = db.conn.execute(
                    "SELECT id FROM pending_changes WHERE photo_id = ? AND change_type = ? AND value = ? AND workspace_id = ?",
                    (pid, "keyword_add", species, ws_id),
                ).fetchone()
                if not existing:
                    db.conn.execute(
                        "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) VALUES (?, ?, ?, ?)",
                        (pid, "keyword_add", species, ws_id),
                    )
            db.conn.commit()
        except Exception:
            db.conn.rollback()
            raise

        # Record edit history
        items = [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)} for pid in photo_ids]
        db.record_edit('keyword_add',
                       f'Confirmed species "{species}" on {len(photo_ids)} photos',
                       str(kid), items, is_batch=len(photo_ids) > 1)

        # Update pipeline cache if it exists
        from pipeline import load_results_raw, save_results_raw

        cache_dir = os.path.dirname(db_path)
        cached = load_results_raw(cache_dir, db._active_workspace_id)
        if cached:
            photo_id_set = set(photo_ids)
            burst_index = body.get("burst_index")
            for enc in cached.get("encounters", []):
                enc_ids = set(enc.get("photo_ids", []))
                if not photo_id_set.issubset(enc_ids):
                    continue
                if burst_index is not None and "bursts" in enc:
                    # Burst-level confirmation
                    if 0 <= burst_index < len(enc["bursts"]):
                        burst = enc["bursts"][burst_index]
                        burst["species_override"] = {
                            "species": species,
                            "confirmed": True,
                        }
                else:
                    # Encounter-level confirmation
                    enc["species_confirmed"] = True
                    enc["confirmed_species"] = species
                break
            save_results_raw(cached, cache_dir, db._active_workspace_id)

        return jsonify({
            "ok": True,
            "species": species,
            "keyword_id": kid,
            "photo_count": len(photo_ids),
        })

    @app.route("/api/species/search")
    def api_species_search():
        """Search species names from active label sets for autocomplete."""
        q = request.args.get("q", "").strip().lower()
        if len(q) < 2:
            return jsonify([])

        from labels import get_active_labels

        matches = []
        seen = set()
        for label_set in get_active_labels():
            labels_file = label_set.get("labels_file", "")
            if not labels_file or not os.path.exists(labels_file):
                continue
            try:
                with open(labels_file) as f:
                    for line in f:
                        name = line.strip()
                        if not name:
                            continue
                        name_lower = name.lower()
                        if q in name_lower and name_lower not in seen:
                            seen.add(name_lower)
                            matches.append(name)
                            if len(matches) >= 20:
                                break
            except Exception:
                pass
            if len(matches) >= 20:
                break

        # Also search existing species keywords in the database
        db = _get_db()
        kw_rows = db.conn.execute(
            "SELECT name FROM keywords WHERE is_species = 1 AND LOWER(name) LIKE ?",
            (f"%{q}%",),
        ).fetchall()
        for row in kw_rows:
            if row["name"].lower() not in seen:
                seen.add(row["name"].lower())
                matches.append(row["name"])

        return jsonify(matches[:20])

    @app.route("/api/pipeline/results")
    def api_pipeline_results():
        """Return the most recent pipeline triage results for the active workspace."""
        from pipeline import load_results

        db = _get_db()
        cache_dir = os.path.dirname(db_path)
        results = load_results(cache_dir, db._active_workspace_id)
        if results is None:
            return json_error("No pipeline results found. Run regroup first.", 404)
        return jsonify(results)

    @app.route("/api/pipeline/photo/<int:photo_id>")
    def api_pipeline_photo_detail(photo_id):
        """Return full pipeline feature detail for a single photo."""
        db = _get_db()
        row = db.conn.execute(
            """SELECT id, filename, timestamp, width, height,
                      mask_path, subject_tenengrad, bg_tenengrad,
                      crop_complete, bg_separation,
                      subject_clip_high, subject_clip_low, subject_y_median,
                      phash_crop, subject_size
               FROM photos WHERE id = ?""",
            (photo_id,),
        ).fetchone()
        if not row:
            return json_error("Photo not found", 404)
        result = dict(row)
        # Get primary detection from detections table
        det = db.conn.execute(
            """SELECT box_x, box_y, box_w, box_h, detector_confidence
               FROM detections
               WHERE photo_id = ? AND workspace_id = ?
               ORDER BY detector_confidence DESC LIMIT 1""",
            (photo_id, db._active_workspace_id),
        ).fetchone()
        if det:
            result["detection_box"] = {
                "x": det["box_x"], "y": det["box_y"],
                "w": det["box_w"], "h": det["box_h"],
            }
            result["detection_conf"] = det["detector_confidence"]
        else:
            result["detection_box"] = None
            result["detection_conf"] = None
        return jsonify(result)

    @app.route("/masks/<filename>")
    def serve_mask(filename):
        """Serve mask PNG files."""
        masks_dir = os.path.join(os.path.dirname(db_path), "masks")
        mask_path = os.path.join(masks_dir, filename)
        if os.path.exists(mask_path):
            return send_from_directory(masks_dir, filename)
        return "", 404

    @app.route("/api/pipeline/reflow", methods=["POST"])
    def api_pipeline_reflow():
        """Re-run stages 4-6 with new scoring/selection thresholds.

        Instant (milliseconds) — no model inference, no regrouping.
        Takes threshold overrides in the request body, re-scores and
        re-triages the existing encounter/burst grouping.
        """
        from pipeline import (
            load_photo_features,
            reflow,
            run_grouping,
            save_results,
            serialize_results,
        )

        body = request.get_json(silent=True) or {}
        overrides = body.get("config", {})

        import config as cfg

        db = _get_db()
        effective_cfg = db.get_effective_config(cfg.load())
        pipeline_cfg = {**effective_cfg.get("pipeline", {}), **overrides}

        # Load features and re-group (grouping is fast, seconds)
        # We re-group to have the full photo dicts with numpy arrays
        # (the cached JSON doesn't have embeddings)
        photos = load_photo_features(db, config=effective_cfg)
        if not photos:
            return json_error("No photos with pipeline features", 404)

        encounters = run_grouping(photos, config=pipeline_cfg)
        results = reflow(encounters, config=pipeline_cfg)

        # Save updated results
        cache_dir = os.path.dirname(db_path)
        save_results(results, cache_dir, db._active_workspace_id)

        return jsonify(serialize_results(results))

    @app.route("/api/pipeline/regroup-live", methods=["POST"])
    def api_pipeline_regroup_live():
        """Re-run stages 2-6 with new grouping thresholds.

        Slightly slower than reflow (seconds) because it re-runs encounter
        segmentation and burst clustering in addition to scoring/triage.
        """
        from pipeline import (
            load_photo_features,
            run_full_pipeline,
            save_results,
            serialize_results,
        )

        body = request.get_json(silent=True) or {}
        overrides = body.get("config", {})

        import config as cfg

        db = _get_db()
        effective_cfg = db.get_effective_config(cfg.load())
        pipeline_cfg = {**effective_cfg.get("pipeline", {}), **overrides}

        photos = load_photo_features(db, config=effective_cfg)
        if not photos:
            return json_error("No photos with pipeline features", 404)

        results = run_full_pipeline(photos, config=pipeline_cfg)

        cache_dir = os.path.dirname(db_path)
        save_results(results, cache_dir, db._active_workspace_id)

        return jsonify(serialize_results(results))

    @app.route("/api/pipeline/detach-burst", methods=["POST"])
    def api_pipeline_detach_burst():
        """Detach a burst from its encounter, creating a new standalone encounter."""
        from pipeline import load_results_raw, rebuild_species_predictions, save_results_raw

        body = request.get_json(silent=True) or {}
        enc_idx = body.get("encounter_index")
        burst_idx = body.get("burst_index")
        if enc_idx is None or burst_idx is None:
            return json_error("encounter_index and burst_index are required")

        db = _get_db()
        cache_dir = os.path.dirname(db_path)
        results = load_results_raw(cache_dir, db._active_workspace_id)
        if results is None:
            return json_error("No pipeline results found", 404)

        encounters = results["encounters"]
        if enc_idx < 0 or enc_idx >= len(encounters):
            return json_error("Invalid encounter_index")
        enc = encounters[enc_idx]
        bursts = enc.get("bursts", [])
        if burst_idx < 0 or burst_idx >= len(bursts):
            return json_error("Invalid burst_index")

        # Remove burst from encounter
        detached = bursts.pop(burst_idx)
        detached_ids = detached["photo_ids"]

        if len(bursts) == 0:
            # Last burst — remove the encounter entirely, detached becomes the encounter
            encounters.pop(enc_idx)
        else:
            # Update encounter metadata and recalculate species predictions
            enc["photo_ids"] = [pid for pid in enc["photo_ids"] if pid not in detached_ids]
            enc["photo_count"] = len(enc["photo_ids"])
            enc["burst_count"] = len(bursts)
            enc["species_predictions"] = rebuild_species_predictions(results, enc["photo_ids"])
            # Recalculate remaining burst predictions too
            for b in bursts:
                b["species_predictions"] = rebuild_species_predictions(results, b["photo_ids"])

        # Create new encounter from detached burst
        new_enc_predictions = rebuild_species_predictions(results, detached_ids)
        # Also refresh the detached burst's own predictions
        detached["species_predictions"] = new_enc_predictions
        new_enc = {
            "species": enc.get("species"),
            "confirmed_species": detached.get("species_override", {}).get("species") if detached.get("species_override") else None,
            "species_predictions": new_enc_predictions,
            "species_confirmed": bool(detached.get("species_override", {}).get("confirmed")) if detached.get("species_override") else False,
            "photo_count": len(detached_ids),
            "burst_count": 1,
            "time_range": [None, None],
            "photo_ids": detached_ids,
            "bursts": [detached],
        }
        encounters.append(new_enc)

        # Update summary
        results["summary"]["encounter_count"] = len(encounters)
        results["summary"]["burst_count"] = sum(
            e.get("burst_count", 0) for e in encounters
        )

        save_results_raw(results, cache_dir, db._active_workspace_id)
        return jsonify({"ok": True, "encounters": encounters, "summary": results["summary"]})

    @app.route("/api/pipeline/detach-photo", methods=["POST"])
    def api_pipeline_detach_photo():
        """Detach a photo from its burst, creating a new single-photo burst."""
        from pipeline import load_results_raw, rebuild_species_predictions, save_results_raw

        body = request.get_json(silent=True) or {}
        enc_idx = body.get("encounter_index")
        burst_idx = body.get("burst_index")
        photo_id = body.get("photo_id")
        if enc_idx is None or burst_idx is None or photo_id is None:
            return json_error("encounter_index, burst_index, and photo_id are required")

        db = _get_db()
        cache_dir = os.path.dirname(db_path)
        results = load_results_raw(cache_dir, db._active_workspace_id)
        if results is None:
            return json_error("No pipeline results found", 404)

        encounters = results["encounters"]
        if enc_idx < 0 or enc_idx >= len(encounters):
            return json_error("Invalid encounter_index")
        enc = encounters[enc_idx]
        bursts = enc.get("bursts", [])
        if burst_idx < 0 or burst_idx >= len(bursts):
            return json_error("Invalid burst_index")

        burst = bursts[burst_idx]
        if photo_id not in burst["photo_ids"]:
            return json_error("photo_id not in burst")

        # Remove photo from burst
        burst["photo_ids"].remove(photo_id)

        if len(burst["photo_ids"]) == 0:
            # Last photo — remove the empty burst
            bursts.pop(burst_idx)
        else:
            # Recalculate source burst predictions without the removed photo
            burst["species_predictions"] = rebuild_species_predictions(results, burst["photo_ids"])

        # Create new single-photo burst in the same encounter
        new_burst = {
            "photo_ids": [photo_id],
            "species_predictions": rebuild_species_predictions(results, [photo_id]),
            "species_override": None,
        }
        bursts.append(new_burst)
        enc["burst_count"] = len(bursts)
        # Recalculate encounter-level predictions
        enc["species_predictions"] = rebuild_species_predictions(results, enc["photo_ids"])

        # Update summary
        results["summary"]["burst_count"] = sum(
            e.get("burst_count", 0) for e in encounters
        )

        save_results_raw(results, cache_dir, db._active_workspace_id)
        return jsonify({"ok": True, "encounters": encounters, "summary": results["summary"]})

    @app.route("/api/pipeline/save-cache", methods=["POST"])
    def api_pipeline_save_cache():
        """Save pipeline results back to cache (used by undo)."""
        from pipeline import save_results_raw

        body = request.get_json(silent=True) or {}
        if not isinstance(body.get("encounters"), list) or not isinstance(body.get("photos"), list):
            return json_error("Invalid pipeline results structure")
        db = _get_db()
        cache_dir = os.path.dirname(db_path)
        save_results_raw(body, cache_dir, db._active_workspace_id)
        return jsonify({"ok": True})

    @app.route("/api/pipeline/config", methods=["GET", "POST"])
    def api_pipeline_config():
        """Get or update pipeline model configuration.

        GET: Returns current effective pipeline config.
        POST: Saves pipeline config to workspace overrides.
              Accepts {sam2_variant, dinov2_variant, proxy_longest_edge}.
        """
        import config as cfg

        db = _get_db()

        if request.method == "GET":
            effective = db.get_effective_config(cfg.load())
            return jsonify(effective.get("pipeline", {}))

        body = request.get_json(silent=True) or {}
        allowed_keys = {"sam2_variant", "dinov2_variant", "proxy_longest_edge"}
        pipeline_updates = {k: v for k, v in body.items() if k in allowed_keys}
        if not pipeline_updates:
            return json_error("No valid pipeline config keys provided")

        # Load current overrides, merge pipeline updates
        ws = db.get_workspace(db._active_workspace_id)
        current_overrides = {}
        if ws and ws["config_overrides"]:
            try:
                current_overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
            except (json.JSONDecodeError, TypeError):
                pass

        pipeline_section = current_overrides.get("pipeline", {})
        pipeline_section.update(pipeline_updates)
        current_overrides["pipeline"] = pipeline_section

        db.update_workspace(db._active_workspace_id, config_overrides=current_overrides)

        return jsonify({"pipeline": pipeline_section, "status": "saved"})

    @app.route("/api/culling/results")
    def api_culling_results():
        """Return the most recent culling analysis results for the active workspace."""
        db = _get_db()
        cache_path = os.path.join(os.path.dirname(db_path), f"culling_results_ws{db._active_workspace_id}.json")
        if not os.path.exists(cache_path):
            return json_error("No culling analysis found. Run one first.", 404)

        with open(cache_path) as f:
            results = json.load(f)

        # Enrich with photo metadata for the UI
        db = _get_db()
        for sg in results["species_groups"]:
            for pg in sg.get("scene_groups", sg.get("pose_groups", [])):
                for photo in pg["photos"]:
                    p = db.get_photo(photo["photo_id"])
                    if p:
                        photo["filename"] = p["filename"]
                        photo["sharpness"] = p["sharpness"]
                        photo["subject_sharpness"] = p["subject_sharpness"]
                        photo["quality_score"] = p["quality_score"]

        return jsonify(results)

    @app.route("/api/culling/apply", methods=["POST"])
    def api_culling_apply():
        """Apply culling decisions — flag keepers and reject others."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        keepers = body.get("keepers", [])
        rejects = body.get("rejects", [])

        # Capture old flags before mutation
        old_flags = {}
        for pid in keepers + rejects:
            old = db.get_photo(pid)
            if old:
                old_flags[pid] = old["flag"] or "none"

        for pid in keepers:
            db.update_photo_flag(pid, "flagged")
        for pid in rejects:
            db.update_photo_flag(pid, "rejected")

        # Record flag history
        flag_items = []
        for pid in keepers:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'flagged'})
        for pid in rejects:
            if pid in old_flags:
                flag_items.append({'photo_id': pid, 'old_value': old_flags[pid], 'new_value': 'rejected'})
        if flag_items:
            db.record_edit('flag',
                           f'Culling: flagged {len(keepers)}, rejected {len(rejects)}',
                           'culling_apply', flag_items, is_batch=True)

        log.info("Culling applied: %d keepers, %d rejects", len(keepers), len(rejects))
        return jsonify({"ok": True, "keepers": len(keepers), "rejects": len(rejects)})

    @app.route("/api/photos/search")
    def api_photo_text_search():
        """Search photos by text query using CLIP cosine similarity."""
        import numpy as np

        query = request.args.get("q", "").strip()
        if not query:
            return json_error("Missing query parameter 'q'")

        limit = request.args.get("limit", 50, type=int)
        threshold = request.args.get("threshold", 0.15, type=float)

        db = _get_db()

        # Determine current model
        from models import get_active_model
        active_model = get_active_model()
        if not active_model:
            return jsonify({"results": [], "total_matches": 0, "model_used": None,
                            "reason": "no_model"})

        model_name = active_model["name"]
        model_type = active_model.get("model_type", "bioclip")

        # timm models don't produce CLIP embeddings — text search unsupported
        if model_type == "timm":
            return jsonify({"results": [], "total_matches": 0, "model_used": model_name,
                            "reason": "model_no_text_search"})

        # Load embeddings for current model
        emb_pairs = db.get_embeddings_by_model(model_name)
        if not emb_pairs:
            return jsonify({"results": [], "total_matches": 0, "model_used": model_name,
                            "reason": "no_embeddings"})

        # Encode query text
        from text_encoder import encode_text
        model_str = active_model["model_str"]
        weights_path = active_model.get("weights_path", "")
        try:
            query_vec = encode_text(query, model_str=model_str, pretrained_str=weights_path)
        except Exception as e:
            log.exception("Text encoding failed for query=%r model=%s", query, model_name)
            return json_error(f"Text encoding failed: {e}", status=500)

        # Build matrix and compute similarities
        photo_ids = [pid for pid, _ in emb_pairs]
        emb_matrix = np.stack(
            [np.frombuffer(blob, dtype=np.float32) for _, blob in emb_pairs]
        )
        similarities = emb_matrix @ query_vec

        # Filter and sort
        mask = similarities >= threshold
        filtered_ids = [photo_ids[i] for i in range(len(photo_ids)) if mask[i]]
        filtered_sims = similarities[mask]
        total_matches = len(filtered_ids)

        # Top-N by similarity
        if total_matches > 0:
            top_indices = np.argsort(filtered_sims)[::-1][:limit]
            top_pids = [filtered_ids[idx] for idx in top_indices]
            top_sims = [float(filtered_sims[idx]) for idx in top_indices]
            photos_map = db.get_photos_by_ids(top_pids)
            results = []
            for pid, sim in zip(top_pids, top_sims, strict=False):
                if pid in photos_map:
                    results.append({
                        "photo": dict(photos_map[pid]),
                        "similarity": round(sim, 4),
                    })
        else:
            results = []

        return jsonify({
            "results": results,
            "total_matches": total_matches,
            "model_used": model_name,
        })

    @app.route("/api/photos/<int:photo_id>/similar")
    def api_photo_similar(photo_id):
        """Find photos with similar embeddings to the given photo."""
        import numpy as np

        db = _get_db()
        limit = request.args.get("limit", 20, type=int)

        # Get the source photo's embedding
        source = db.conn.execute(
            "SELECT embedding FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()
        if not source or not source["embedding"]:
            return (
                jsonify(
                    {"error": "No embedding for this photo — run classification first"}
                ),
                400,
            )

        source_emb = np.frombuffer(source["embedding"], dtype=np.float32)

        # Load all embeddings (excluding source photo)
        rows = db.conn.execute(
            """SELECT p.id, p.embedding FROM photos p
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE p.embedding IS NOT NULL AND p.id != ? AND wf.workspace_id = ?""",
            (photo_id, db._active_workspace_id),
        ).fetchall()

        if not rows:
            return jsonify({"similar": [], "total_compared": 0})

        # Compute cosine similarities (embeddings are already normalized)
        photo_ids = []
        embeddings = []
        for row in rows:
            photo_ids.append(row["id"])
            embeddings.append(np.frombuffer(row["embedding"], dtype=np.float32))

        emb_matrix = np.stack(embeddings)
        similarities = emb_matrix @ source_emb

        # Get top-N most similar
        top_indices = np.argsort(similarities)[::-1][:limit]
        top_pids = [photo_ids[idx] for idx in top_indices]
        top_sims = [float(similarities[idx]) for idx in top_indices]
        photos_map = db.get_photos_by_ids(top_pids)

        results = []
        for pid, sim in zip(top_pids, top_sims, strict=False):
            if pid in photos_map:
                results.append(
                    {
                        "photo": dict(photos_map[pid]),
                        "similarity": round(sim, 4),
                    }
                )

        return jsonify(
            {
                "similar": results,
                "total_compared": len(rows),
            }
        )

    @app.route("/api/photos/<int:photo_id>/pipeline")
    def api_photo_pipeline(photo_id):
        """Return full pipeline debug info for a single photo."""
        db = _get_db()
        photo = db.conn.execute(
            """SELECT p.*, f.path as folder_path FROM photos p
               JOIN folders f ON f.id = p.folder_id WHERE p.id = ?""",
            (photo_id,),
        ).fetchone()
        if not photo:
            return json_error("Photo not found", 404)

        result = dict(photo)
        # Remove binary embedding and dead detection columns from response
        result.pop("embedding", None)
        result.pop("detection_box", None)
        result.pop("detection_conf", None)

        # Get primary detection from detections table
        det = db.conn.execute(
            """SELECT box_x, box_y, box_w, box_h, detector_confidence
               FROM detections
               WHERE photo_id = ? AND workspace_id = ?
               ORDER BY detector_confidence DESC LIMIT 1""",
            (photo_id, db._active_workspace_id),
        ).fetchone()
        if det:
            result["detection_box"] = {
                "x": det["box_x"], "y": det["box_y"],
                "w": det["box_w"], "h": det["box_h"],
            }
            result["detection_conf"] = det["detector_confidence"]

        # Get detections for this photo (from detections table)
        dets = db.get_detections(photo_id)
        result["detections"] = [dict(d) for d in dets]

        # Get predictions for this photo (through detections JOIN)
        preds = db.conn.execute(
            """SELECT pr.species, pr.confidence, pr.model, pr.category, pr.status,
                      pr.group_id, pr.vote_count, pr.total_votes, pr.individual,
                      d.box_x, d.box_y, d.box_w, d.box_h, d.detector_confidence
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE d.photo_id = ? AND d.workspace_id = ?
               ORDER BY pr.confidence DESC""",
            (photo_id, db._active_workspace_id),
        ).fetchall()
        result["predictions"] = [dict(p) for p in preds]

        # Get keywords
        keywords = db.get_photo_keywords(photo_id)
        result["keywords"] = [dict(k) for k in keywords]

        # Compute crop info from primary detection (highest confidence)
        primary_det = dets[0] if dets else None
        if primary_det:
            import config as cfg
            box = {"x": primary_det["box_x"], "y": primary_det["box_y"],
                   "w": primary_det["box_w"], "h": primary_det["box_h"]}
            pad = cfg.load().get("detection_padding", 0.2)
            result["crop_box"] = {
                "x": max(0, box["x"] - box["w"] * pad),
                "y": max(0, box["y"] - box["h"] * pad),
                "w": min(1.0, box["w"] * (1 + 2 * pad)),
                "h": min(1.0, box["h"] * (1 + 2 * pad)),
            }

        return jsonify(result)

    @app.route("/photos/<int:photo_id>/crop")
    def serve_crop_preview(photo_id):
        """Serve the cropped region that would be sent to BioCLIP."""
        import config as cfg
        from image_loader import load_image
        from PIL import Image

        db = _get_db()
        photo = db.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
            (photo_id,),
        ).fetchone()
        if not photo:
            return "Not found", 404

        image_path = os.path.join(photo["path"], photo["filename"])
        img = load_image(image_path, max_size=None)
        if img is None:
            return "Could not load image", 500

        # Get primary detection box from detections table
        det_row = db.conn.execute(
            """SELECT box_x, box_y, box_w, box_h
               FROM detections
               WHERE photo_id = ? AND workspace_id = ?
               ORDER BY detector_confidence DESC LIMIT 1""",
            (photo_id, db._active_workspace_id),
        ).fetchone()
        det_box = None
        if det_row:
            det_box = {
                "x": det_row["box_x"], "y": det_row["box_y"],
                "w": det_row["box_w"], "h": det_row["box_h"],
            }
        if det_box:
            iw, ih = img.size
            padding = cfg.load().get("detection_padding", 0.2)
            pad_w = det_box["w"] * padding
            pad_h = det_box["h"] * padding
            x1 = max(0, int((det_box["x"] - pad_w) * iw))
            y1 = max(0, int((det_box["y"] - pad_h) * ih))
            x2 = min(iw, int((det_box["x"] + det_box["w"] + pad_w) * iw))
            y2 = min(ih, int((det_box["y"] + det_box["h"] + pad_h) * ih))
            crop = img.crop((x1, y1, x2, y2))
            if crop.size[0] >= 50 and crop.size[1] >= 50:
                img = crop

        img.thumbnail((800, 800), Image.LANCZOS)
        import io

        preview_quality = cfg.load().get("preview_quality", 90)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=preview_quality)
        buf.seek(0)
        return Response(buf.read(), mimetype="image/jpeg")

    @app.route("/photos/<int:photo_id>/full")
    def serve_full_photo(photo_id):
        """Serve a display-sized preview, cached on first view."""
        import config as cfg
        from flask import send_file

        preview_dir = os.path.join(
            os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
        )
        max_size = cfg.get("preview_max_size") or 1920
        if max_size == 0:
            max_size = None  # Full resolution
        cache_path = os.path.join(preview_dir, f"{photo_id}.jpg")

        # Serve from cache if available
        if os.path.exists(cache_path):
            return send_file(cache_path, mimetype="image/jpeg")

        # Generate and cache
        from image_loader import load_image

        db = _get_db()
        photo = db.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
            (photo_id,),
        ).fetchone()
        if not photo:
            return "Not found", 404
        image_path = os.path.join(photo["path"], photo["filename"])
        img = load_image(image_path, max_size=max_size)
        if img is None:
            return "Could not load image", 500

        os.makedirs(preview_dir, exist_ok=True)
        preview_quality = cfg.load().get("preview_quality", 90)
        img.save(cache_path, format="JPEG", quality=preview_quality)
        return send_file(cache_path, mimetype="image/jpeg")

    @app.route("/photos/<int:photo_id>/original")
    def serve_original_photo(photo_id):
        """Serve the full-resolution image for 1:1 zoom, converting RAW formats to JPEG."""
        import config as cfg
        from flask import send_file

        db = _get_db()
        photo = db.conn.execute(
            "SELECT p.filename, f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
            (photo_id,),
        ).fetchone()
        if not photo:
            return "Not found", 404
        image_path = os.path.join(photo["path"], photo["filename"])
        if not os.path.exists(image_path):
            return "Not found", 404

        # Serve browser-native formats directly
        ext = os.path.splitext(photo["filename"])[1].lower()
        if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"):
            return send_file(image_path)

        # Convert non-browser formats (RAW, etc.) to JPEG, cached
        originals_dir = os.path.join(
            os.path.dirname(app.config["THUMB_CACHE_DIR"]), "originals"
        )
        cache_path = os.path.join(originals_dir, f"{photo_id}.jpg")
        if os.path.exists(cache_path):
            return send_file(cache_path, mimetype="image/jpeg")

        from image_loader import load_image

        img = load_image(image_path, max_size=None)
        if img is None:
            return "Could not load image", 500

        os.makedirs(originals_dir, exist_ok=True)
        preview_quality = cfg.load().get("preview_quality", 90)
        img.save(cache_path, format="JPEG", quality=preview_quality)
        return send_file(cache_path, mimetype="image/jpeg")

    # -- Logs page --

    @app.route("/logs")
    def logs_page():
        return render_template("logs.html")

    @app.route("/map")
    def map_page():
        return render_template("map.html", active_page="map")

    @app.route("/dashboard")
    def dashboard_page():
        return render_template("stats.html")

    @app.route("/stats")
    def stats_redirect():
        return redirect("/dashboard")

    return app


def main():
    parser = argparse.ArgumentParser(description="Vireo Photo Browser")
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/.vireo/vireo.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--thumb-dir",
        default=os.path.expanduser("~/.vireo/thumbnails"),
        help="Path to thumbnail cache directory",
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument(
        "--load-taxonomy",
        action="store_true",
        help="Download and import the iNaturalist taxonomy, then exit",
    )
    args = parser.parse_args()

    if args.load_taxonomy:
        from db import Database
        from taxonomy import fetch_common_names, load_taxonomy, seed_informal_groups
        db = Database(args.db)
        log.info("Loading taxonomy tree from iNaturalist...")
        stats = load_taxonomy(db)
        log.info("  Taxonomy: %d taxa loaded, %d skipped", stats['loaded'], stats['skipped'])
        log.info("Fetching common names from iNat API (this may take a few minutes)...")
        cn_stats = fetch_common_names(db)
        log.info("  Common names: %d taxa updated", cn_stats['updated'])
        log.info("Seeding informal groups...")
        ig_stats = seed_informal_groups(db)
        log.info("  Informal groups: %d groups created", ig_stats['groups_created'])
        log.info("Done.")
        raise SystemExit(0)

    # Resolve port: --port 0 means pick a random free port
    port = args.port
    if port == 0:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

    # Write the port file when random port was requested (for Tauri to discover)
    if args.port == 0:
        port_file = os.path.join(os.path.expanduser("~/.vireo"), "port")
        with open(port_file, "w") as f:
            f.write(str(port))
        log.info("Random port %d written to %s", port, port_file)

    app = create_app(db_path=args.db, thumb_cache_dir=args.thumb_dir)

    # Startup banner
    import config as cfg
    startup_cfg = cfg.load()
    log.info("=" * 50)
    log.info("Vireo starting on http://localhost:%d", port)
    log.info("  Database: %s", args.db)
    log.info("  Thumbnails: %s", args.thumb_dir)
    log.info("  Threshold: %.0f%%  Grouping: %ds  Similarity: %.0f%%",
             startup_cfg.get("classification_threshold", 0.4) * 100,
             startup_cfg.get("grouping_window_seconds", 10),
             startup_cfg.get("similarity_threshold", 0.85) * 100)
    if startup_cfg.get("hf_token"):
        log.info("  HuggingFace token: configured")
    log.info("=" * 50)

    # Open browser after server is ready, not before
    if not args.no_browser:
        import threading
        import urllib.request

        def _open_browser():
            url = f"http://localhost:{port}"
            for _ in range(50):  # try for up to 5 seconds
                try:
                    urllib.request.urlopen(url, timeout=0.1)
                    webbrowser.open(url)
                    return
                except Exception:
                    time.sleep(0.1)

        threading.Thread(target=_open_browser, daemon=True).start()

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
