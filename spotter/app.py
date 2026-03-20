"""Flask web app for the Spotter photo browser.

Usage:
    python spotter/app.py --db ~/.spotter/spotter.db [--port 8080]
"""

import argparse
import logging
import logging.handlers
import os
import sys
import webbrowser

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lr-migration"))

import json
import queue
import time

from db import Database
from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
)
from jobs import JobRunner, LogBroadcaster

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# File logging with rotation — persists across restarts
_log_dir = os.path.expanduser("~/.spotter")
os.makedirs(_log_dir, exist_ok=True)
_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(_log_dir, "spotter.log"),
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


def create_app(db_path, thumb_cache_dir=None):
    """Create the Flask app for the Spotter photo browser.

    Args:
        db_path: path to the SQLite database
        thumb_cache_dir: path to thumbnail cache directory
    """
    app = Flask(
        __name__, template_folder=os.path.join(os.path.dirname(__file__), "templates")
    )
    app.config["DB_PATH"] = db_path
    app.config["THUMB_CACHE_DIR"] = thumb_cache_dir or os.path.expanduser(
        "~/.spotter/thumbnails"
    )

    # Request timing middleware — logs slow requests
    @app.before_request
    def _start_timer():
        request._start_time = time.time()

    @app.after_request
    def _log_requests(response):
        if hasattr(request, "_start_time"):
            elapsed = time.time() - request._start_time
            # Log all POST/DELETE actions (user-initiated) and slow requests
            if request.method in ("POST", "DELETE"):
                log.info(
                    "Action: %s %s → %s (%.1fs)",
                    request.method,
                    request.path,
                    response.status_code,
                    elapsed,
                )
            elif elapsed > 0.5:
                log.warning(
                    "Slow request: %s %s took %.1fs",
                    request.method,
                    request.path,
                    elapsed,
                )
        return response

    def _get_db():
        """Get a Database instance. Creates a new connection per request."""
        if not hasattr(app, "_db") or app._db is None:
            app._db = Database(db_path)
        return app._db

    # Load user config (e.g. HF token) on startup
    import config as cfg

    startup_cfg = cfg.load()
    if startup_cfg.get("hf_token"):
        os.environ["HF_TOKEN"] = startup_cfg["hf_token"]

    # Initialize job runner, log broadcaster, and default collections
    init_db = Database(db_path)
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

    app._job_runner = JobRunner(db=init_db)
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    # -- Page routes --

    @app.route("/")
    def index():
        return redirect("/browse")

    @app.route("/browse")
    def browse():
        return render_template("browse.html")

    @app.route("/classify")
    def classify():
        return render_template("classify.html")

    @app.route("/review")
    def review():
        return render_template("review.html")

    @app.route("/import")
    def import_page():
        return render_template("import.html")

    @app.route("/audit")
    def audit():
        return render_template("audit.html")

    @app.route("/variants")
    def variants_page():
        return render_template("variants.html")

    @app.route("/settings")
    def settings():
        return render_template("settings.html")

    # -- API routes --

    @app.route("/api/browse/init")
    def api_browse_init():
        """Combined endpoint for browse page initial load — one request instead of five."""
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 50, type=int)
        sort = request.args.get("sort", "date")

        photos = db.get_photos(page=page, per_page=per_page, sort=sort)
        total = db.count_photos()
        folders = db.get_folder_tree()
        keywords = db.get_keyword_tree()
        collections = db.get_collections()

        return jsonify(
            {
                "photos": [dict(p) for p in photos],
                "total": total,
                "page": page,
                "per_page": per_page,
                "folders": [dict(f) for f in folders],
                "keywords": [dict(k) for k in keywords],
                "collections": [dict(c) for c in collections],
            }
        )

    @app.route("/api/folders")
    def api_folders():
        db = _get_db()
        folders = db.get_folder_tree()
        return jsonify([dict(f) for f in folders])

    @app.route("/api/photos")
    def api_photos():
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 50, type=int)
        sort = request.args.get("sort", "date")
        folder_id = request.args.get("folder_id", None, type=int)
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)

        photos = db.get_photos(
            folder_id=folder_id,
            page=page,
            per_page=per_page,
            sort=sort,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
        )

        # Total count — use count_photos for unfiltered, otherwise count the filtered set
        if not any([folder_id, rating_min, date_from, date_to, keyword]):
            total = db.count_photos()
        else:
            total = len(
                db.get_photos(
                    folder_id=folder_id,
                    rating_min=rating_min,
                    date_from=date_from,
                    date_to=date_to,
                    keyword=keyword,
                    per_page=999999,
                )
            )

        return jsonify(
            {
                "photos": [dict(p) for p in photos],
                "total": total,
                "page": page,
                "per_page": per_page,
            }
        )

    @app.route("/api/photos/<int:photo_id>")
    def api_photo_detail(photo_id):
        db = _get_db()
        photo = db.get_photo(photo_id)
        if not photo:
            return jsonify({"error": "not found"}), 404

        result = dict(photo)
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
                from compare import read_xmp_keywords

                xmp_keywords = read_xmp_keywords(xmp_path)
            result["xmp_exists"] = xmp_exists
            result["xmp_keywords"] = xmp_keywords
            result["xmp_path"] = xmp_path
        else:
            result["xmp_exists"] = False
            result["xmp_keywords"] = []
            result["xmp_path"] = ""

        return jsonify(result)

    @app.route("/api/keywords")
    def api_keywords():
        db = _get_db()
        keywords = db.get_keyword_tree()
        return jsonify([dict(k) for k in keywords])

    # -- Undo stack (in-memory, session-only) --
    _undo_stack = []
    _max_undo = 50

    def _push_undo(action):
        _undo_stack.append(action)
        if len(_undo_stack) > _max_undo:
            _undo_stack.pop(0)

    # -- Edit API routes --

    @app.route("/api/photos/<int:photo_id>/rating", methods=["POST"])
    def api_set_rating(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        rating = body.get("rating", 0)
        old = db.get_photo(photo_id)
        old_rating = old["rating"] if old else 0
        db.update_photo_rating(photo_id, rating)
        db.queue_change(photo_id, "rating", str(rating))
        _push_undo(
            {
                "type": "rating",
                "photo_ids": [photo_id],
                "old_value": old_rating,
                "new_value": rating,
                "description": f"Set rating to {rating}",
            }
        )
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/flag", methods=["POST"])
    def api_set_flag(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        flag = body.get("flag", "none")
        old = db.get_photo(photo_id)
        old_flag = old["flag"] if old else "none"
        db.update_photo_flag(photo_id, flag)
        db.queue_change(photo_id, "flag", flag)
        _push_undo(
            {
                "type": "flag",
                "photo_ids": [photo_id],
                "old_value": old_flag,
                "new_value": flag,
                "description": f"Set flag to {flag}",
            }
        )
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/keywords", methods=["POST"])
    def api_add_keyword(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return jsonify({"error": "name required"}), 400
        kid = db.add_keyword(name)
        db.tag_photo(photo_id, kid)
        db.queue_change(photo_id, "keyword_add", name)
        _push_undo(
            {
                "type": "keyword_add",
                "photo_ids": [photo_id],
                "keyword_id": kid,
                "keyword_name": name,
                "description": f'Added keyword "{name}"',
            }
        )
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
        db.queue_change(photo_id, "keyword_remove", kw_name)
        _push_undo(
            {
                "type": "keyword_remove",
                "photo_ids": [photo_id],
                "keyword_id": keyword_id,
                "keyword_name": kw_name,
                "description": f'Removed keyword "{kw_name}"',
            }
        )
        return jsonify({"ok": True})

    # -- Batch operations --

    @app.route("/api/batch/rating", methods=["POST"])
    def api_batch_rating():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        rating = body.get("rating", 0)
        if not photo_ids:
            return jsonify({"error": "photo_ids required"}), 400
        old_values = {}
        for pid in photo_ids:
            old = db.get_photo(pid)
            if old:
                old_values[pid] = old["rating"]
                db.update_photo_rating(pid, rating)
                db.queue_change(pid, "rating", str(rating))
        _push_undo(
            {
                "type": "batch_rating",
                "photo_ids": photo_ids,
                "old_values": old_values,
                "new_value": rating,
                "description": f"Set rating to {rating} on {len(photo_ids)} photos",
            }
        )
        return jsonify({"ok": True, "updated": len(old_values)})

    @app.route("/api/batch/flag", methods=["POST"])
    def api_batch_flag():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        flag = body.get("flag", "none")
        if not photo_ids:
            return jsonify({"error": "photo_ids required"}), 400
        old_values = {}
        for pid in photo_ids:
            old = db.get_photo(pid)
            if old:
                old_values[pid] = old["flag"]
                db.update_photo_flag(pid, flag)
                db.queue_change(pid, "flag", flag)
        _push_undo(
            {
                "type": "batch_flag",
                "photo_ids": photo_ids,
                "old_values": old_values,
                "new_value": flag,
                "description": f"Set flag to {flag} on {len(photo_ids)} photos",
            }
        )
        return jsonify({"ok": True, "updated": len(old_values)})

    @app.route("/api/batch/keyword", methods=["POST"])
    def api_batch_keyword():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        name = body.get("name", "").strip()
        if not photo_ids or not name:
            return jsonify({"error": "photo_ids and name required"}), 400
        kid = db.add_keyword(name)
        for pid in photo_ids:
            db.tag_photo(pid, kid)
            db.queue_change(pid, "keyword_add", name)
        _push_undo(
            {
                "type": "batch_keyword_add",
                "photo_ids": photo_ids,
                "keyword_id": kid,
                "keyword_name": name,
                "description": f'Added "{name}" to {len(photo_ids)} photos',
            }
        )
        return jsonify({"ok": True, "updated": len(photo_ids)})

    # -- Undo --

    @app.route("/api/undo", methods=["POST"])
    def api_undo():
        if not _undo_stack:
            return jsonify({"error": "nothing to undo"}), 400
        db = _get_db()
        action = _undo_stack.pop()

        if action["type"] == "rating":
            for pid in action["photo_ids"]:
                db.update_photo_rating(pid, action["old_value"])
        elif action["type"] == "flag":
            for pid in action["photo_ids"]:
                db.update_photo_flag(pid, action["old_value"])
        elif action["type"] == "keyword_add":
            for pid in action["photo_ids"]:
                db.untag_photo(pid, action["keyword_id"])
        elif action["type"] == "keyword_remove":
            for pid in action["photo_ids"]:
                db.tag_photo(pid, action["keyword_id"])
        elif action["type"] == "batch_rating":
            for pid, old_val in action["old_values"].items():
                db.update_photo_rating(int(pid), old_val)
        elif action["type"] == "batch_flag":
            for pid, old_val in action["old_values"].items():
                db.update_photo_flag(int(pid), old_val)
        elif action["type"] == "batch_keyword_add":
            for pid in action["photo_ids"]:
                db.untag_photo(pid, action["keyword_id"])

        return jsonify({"ok": True, "undone": action["description"]})

    @app.route("/api/undo/status")
    def api_undo_status():
        if not _undo_stack:
            return jsonify({"available": False, "description": ""})
        return jsonify(
            {
                "available": True,
                "description": _undo_stack[-1]["description"],
                "count": len(_undo_stack),
            }
        )

    # -- Statistics --

    @app.route("/api/stats")
    def api_stats():
        db = _get_db()
        # Top keywords by photo count
        top_keywords = db.conn.execute(
            """
            SELECT k.name, k.is_species, COUNT(pk.photo_id) as photo_count
            FROM keywords k
            JOIN photo_keywords pk ON pk.keyword_id = k.id
            GROUP BY k.id
            ORDER BY photo_count DESC
            LIMIT 30
        """
        ).fetchall()

        # Photos by month
        photos_by_month = db.conn.execute(
            """
            SELECT substr(timestamp, 1, 7) as month, COUNT(*) as count
            FROM photos
            WHERE timestamp IS NOT NULL
            GROUP BY month
            ORDER BY month
        """
        ).fetchall()

        # Rating distribution
        rating_dist = db.conn.execute(
            """
            SELECT rating, COUNT(*) as count
            FROM photos
            GROUP BY rating
            ORDER BY rating
        """
        ).fetchall()

        # Flag distribution
        flag_dist = db.conn.execute(
            """
            SELECT flag, COUNT(*) as count
            FROM photos
            GROUP BY flag
        """
        ).fetchall()

        return jsonify(
            {
                "top_keywords": [dict(r) for r in top_keywords],
                "photos_by_month": [dict(r) for r in photos_by_month],
                "rating_distribution": [dict(r) for r in rating_dist],
                "flag_distribution": [dict(r) for r in flag_dist],
            }
        )

    @app.route("/api/sync/status")
    def api_sync_status():
        db = _get_db()
        changes = db.get_pending_changes()
        return jsonify(
            {
                "pending_count": len(changes),
            }
        )

    # -- Collection API routes --

    @app.route("/api/collections")
    def api_collections():
        db = _get_db()
        collections = db.get_collections()
        return jsonify([dict(c) for c in collections])

    @app.route("/api/collections", methods=["POST"])
    def api_create_collection():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        import json

        name = body.get("name", "").strip()
        rules = body.get("rules", [])
        if not name:
            return jsonify({"error": "name required"}), 400
        cid = db.add_collection(name, json.dumps(rules))
        return jsonify({"ok": True, "id": cid})

    @app.route("/api/collections/<int:collection_id>", methods=["DELETE"])
    def api_delete_collection(collection_id):
        db = _get_db()
        db.delete_collection(collection_id)
        return jsonify({"ok": True})

    @app.route("/api/collections/<int:collection_id>/photos")
    def api_collection_photos(collection_id):
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 50, type=int)
        photos = db.get_collection_photos(collection_id, page=page, per_page=per_page)
        return jsonify(
            {
                "photos": [dict(p) for p in photos],
                "page": page,
                "per_page": per_page,
            }
        )

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
        return jsonify([dict(p) for p in preds])

    @app.route("/api/predictions/<int:pred_id>/accept", methods=["POST"])
    def api_accept_prediction(pred_id):
        db = _get_db()
        db.accept_prediction(pred_id)
        return jsonify({"ok": True})

    @app.route("/api/predictions/<int:pred_id>/reject", methods=["POST"])
    def api_reject_prediction(pred_id):
        db = _get_db()
        db.update_prediction_status(pred_id, "rejected")
        return jsonify({"ok": True})

    @app.route("/api/predictions/group/<group_id>")
    def api_prediction_group(group_id):
        """Get all predictions and photo data for a burst group."""
        db = _get_db()
        preds = db.conn.execute(
            """SELECT pr.*, p.filename, p.timestamp, p.sharpness,
                      p.quality_score, p.subject_sharpness, p.subject_size,
                      p.detection_conf, p.rating, p.flag
               FROM predictions pr
               JOIN photos p ON p.id = pr.photo_id
               WHERE pr.group_id = ?
               ORDER BY p.quality_score DESC""",
            (group_id,),
        ).fetchall()
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

        # Flag picks and add species keyword
        if species:
            kid = db.add_keyword(species, is_species=True)
            for pid in picks:
                db.update_photo_flag(pid, "flagged")
                db.tag_photo(pid, kid)
                db.queue_change(pid, "keyword_add", species)

        # Reject rejects
        for pid in rejects:
            db.update_photo_flag(pid, "rejected")

        # Mark all predictions in this group as accepted
        for pid in picks:
            db.conn.execute(
                "UPDATE predictions SET status = 'accepted' WHERE photo_id = ?",
                (pid,),
            )
        for pid in rejects:
            db.conn.execute(
                "UPDATE predictions SET status = 'rejected' WHERE photo_id = ?",
                (pid,),
            )

        # Remove predictions from group
        for pred_id in removed:
            db.conn.execute(
                "UPDATE predictions SET group_id = NULL WHERE id = ?",
                (pred_id,),
            )

        db.conn.commit()
        return jsonify({"ok": True})

    @app.route("/api/classify/readiness")
    def api_classify_readiness():
        """Check what's ready for classification and what will need work."""
        from classifier import _embedding_cache_path
        from labels import get_active_labels, get_saved_labels
        from models import get_active_model, get_models

        model_id = request.args.get("model_id", "")
        labels_file = request.args.get("labels_file", "")

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
        needs_download = not model_ready and model_source.startswith("hf-hub:")

        # Resolve labels
        use_tol = False
        label_count = 0
        label_name = ""
        if labels_file:
            if os.path.exists(labels_file):
                with open(labels_file) as f:
                    label_count = sum(1 for line in f if line.strip())
                # Find display name
                for ls in get_saved_labels():
                    if ls.get("labels_file") == labels_file:
                        label_name = ls.get("name", labels_file)
                        break
        else:
            active = get_active_labels()
            if active and os.path.exists(active.get("labels_file", "")):
                labels_file = active["labels_file"]
                label_name = active.get("name", "")
                label_count = active.get("species_count", 0)
            else:
                use_tol = True
                label_name = "Tree of Life (all species)"

        # Check embedding cache
        embeddings_cached = False
        if model and not use_tol and labels_file and os.path.exists(labels_file):
            with open(labels_file) as f:
                labels = [line.strip() for line in f if line.strip()]
            if labels:
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
            }
        )

    @app.route("/api/classify/config")
    def api_classify_config():
        """Return classifier configuration from model registry."""
        import config as cfg
        from models import get_active_model, get_taxonomy_info

        active = get_active_model()
        tax = get_taxonomy_info()
        user_cfg = cfg.load()
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
        for key in body:
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

        # HuggingFace cache — only count Spotter-relevant models
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
            return jsonify({"error": "Unknown cache type"}), 400

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
            if f.endswith(".pt"):
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
            return jsonify({"error": "model_id and labels_file required"}), 400

        runner = app._job_runner

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

            # This will compute and cache the embeddings
            Classifier(
                labels=labels,
                model_str=model["model_str"],
                pretrained_str=model["weights_path"],
            )

            return {"labels": len(labels), "model": model["name"]}

        job_id = runner.start(
            "precompute-embeddings",
            work,
            config={
                "model_id": model_id,
                "labels_file": labels_file,
            },
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
        return jsonify({"version": "26.3.1"})

    # -- Import API routes --

    @app.route("/api/import/preview", methods=["POST"])
    def api_import_preview():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        if not catalogs:
            return jsonify({"error": "catalogs required"}), 400
        try:
            from importer import preview_import

            result = preview_import(catalogs, db)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

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
        return jsonify({"error": "Model not found"}), 404

    @app.route("/api/models/active", methods=["POST"])
    def api_set_active_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return jsonify({"error": "model_id required"}), 400
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
            return jsonify({"error": "name and weights_path required"}), 400
        from models import register_model

        model_id = "custom-" + name.lower().replace(" ", "-")
        register_model(model_id, name, model_str, weights_path, "Custom model")
        return jsonify({"ok": True, "model_id": model_id})

    @app.route("/api/jobs/download-model", methods=["POST"])
    def api_job_download_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return jsonify({"error": "model_id required"}), 400

        runner = app._job_runner

        def work(job):
            from models import download_model

            def progress_cb(msg, current=0, total=0):
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
                        "rate": 0,
                        "phase": "Downloading model",
                    },
                )

            path = download_model(model_id, progress_callback=progress_cb)
            return {"model_id": model_id, "weights_path": path}

        job_id = runner.start("download-model", work, config={"model_id": model_id})
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/download-hf-model", methods=["POST"])
    def api_job_download_hf_model():
        body = request.get_json(silent=True) or {}
        repo_id = body.get("repo_id", "").strip()
        if not repo_id:
            return jsonify({"error": "repo_id required"}), 400

        runner = app._job_runner

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

        job_id = runner.start("download-model", work, config={"repo_id": repo_id})
        return jsonify({"job_id": job_id})

    @app.route("/api/taxonomy/info")
    def api_taxonomy_info():
        from models import get_taxonomy_info

        return jsonify(get_taxonomy_info())

    @app.route("/api/jobs/download-taxonomy", methods=["POST"])
    def api_job_download_taxonomy():
        runner = app._job_runner

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

        job_id = runner.start("download-taxonomy", work)
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

    @app.route("/api/labels")
    def api_labels_list():
        from labels import get_active_labels, get_saved_labels

        saved = get_saved_labels()
        active = get_active_labels()
        return jsonify(
            {
                "labels": saved,
                "active": active,
            }
        )

    @app.route("/api/labels/active", methods=["POST"])
    def api_set_active_labels():
        body = request.get_json(silent=True) or {}
        labels_file = body.get("labels_file")
        if not labels_file:
            return jsonify({"error": "labels_file required"}), 400
        from labels import set_active_labels

        set_active_labels(labels_file)
        return jsonify({"ok": True})

    @app.route("/api/jobs/fetch-labels", methods=["POST"])
    def api_job_fetch_labels():
        body = request.get_json(silent=True) or {}
        place_id = body.get("place_id")
        place_name = body.get("place_name", "")
        taxon_groups = body.get("taxon_groups", ["birds"])
        name = body.get("name", "")
        if not place_id:
            return jsonify({"error": "place_id required"}), 400
        if not name:
            group_names = ", ".join(g.title() for g in taxon_groups)
            name = f"{place_name} {group_names}".strip()

        runner = app._job_runner

        def work(job):
            from labels import fetch_species_list, save_labels, set_active_labels

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
                place_id, taxon_groups, progress_callback=progress_cb
            )
            if not species:
                raise RuntimeError(
                    "No species found for this region and taxa selection"
                )
            labels_path = save_labels(name, place_id, place_name, taxon_groups, species)
            set_active_labels(labels_path)

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
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/system/info")
    def api_system_info():
        """Return system information: GPU, Python, PyTorch."""
        import platform

        info = {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "device": "CPU",
            "device_detail": "No GPU acceleration",
            "torch_version": None,
            "torch_detail": "",
        }
        try:
            import torch

            info["torch_version"] = torch.__version__
            if torch.cuda.is_available():
                info["device"] = "CUDA"
                info["device_detail"] = torch.cuda.get_device_name(0)
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                info["device"] = "MPS"
                info["device_detail"] = "Apple Metal Performance Shaders"
            else:
                info["device"] = "CPU"
                info["device_detail"] = "GPU not available — using CPU"
            info["torch_detail"] = (
                f"CUDA: {torch.cuda.is_available()}, MPS: {getattr(torch.backends, 'mps', None) and torch.backends.mps.is_available()}"
            )
        except ImportError:
            info["torch_detail"] = "PyTorch not installed"
        return jsonify(info)

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
            return jsonify({"error": "root path required"}), 400
        if not os.path.isdir(root):
            return jsonify({"error": f"directory not found: {root}"}), 400

        # Remember this scan root (skip temp directories from tests)
        import tempfile

        import config as cfg

        tmp_prefix = tempfile.gettempdir()
        if not root.startswith(tmp_prefix):
            user_cfg = cfg.load()
            roots = user_cfg.get("scan_roots", [])
            if root not in roots:
                roots.insert(0, root)
                user_cfg["scan_roots"] = roots
                cfg.save(user_cfg)

        runner = app._job_runner

        def work(job):
            from scanner import scan as do_scan

            thread_db = Database(db_path)

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
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
                    },
                )

            job["_start_time"] = time.time()
            do_scan(
                root, thread_db, progress_callback=progress_cb, incremental=incremental
            )
            photo_count = thread_db.count_photos()

            # Auto-generate thumbnails after scan
            from thumbnails import generate_all

            log.info("Generating thumbnails...")
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": photo_count,
                    "current_file": "Generating thumbnails...",
                    "rate": 0,
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
                        "current_file": "Generating thumbnails...",
                        "rate": round(
                            current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                    },
                )

            thumb_result = generate_all(
                thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=thumb_cb
            )

            return {"photos_indexed": photo_count, "thumbnails": thumb_result}

        job_id = runner.start(
            "scan", work, config={"root": root, "incremental": incremental}
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/thumbnails", methods=["POST"])
    def api_job_thumbnails():
        runner = app._job_runner

        def work(job):
            from thumbnails import generate_all

            thread_db = Database(db_path)

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

        job_id = runner.start("thumbnails", work)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/previews", methods=["POST"])
    def api_job_previews():
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        runner = app._job_runner

        def work(job):
            import config as cfg
            from image_loader import load_image

            thread_db = Database(db_path)
            max_size = cfg.get("preview_max_size") or 1920
            if max_size == 0:
                max_size = None  # Full resolution
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
                        img.save(cache_path, format="JPEG", quality=90)
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

        job_id = runner.start("previews", work, config={"collection_id": collection_id})
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/import", methods=["POST"])
    def api_job_import():
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        strategy = body.get("strategy", "merge_all")
        write_xmp = body.get("write_xmp", False)
        if not catalogs:
            return jsonify({"error": "catalogs required"}), 400

        runner = app._job_runner

        def work(job):
            from importer import execute_import

            thread_db = Database(db_path)

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
            "import", work, config={"catalogs": catalogs, "strategy": strategy}
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/sync", methods=["POST"])
    def api_job_sync():
        runner = app._job_runner

        def work(job):
            from sync import sync_to_xmp

            thread_db = Database(db_path)

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

        job_id = runner.start("sync", work)
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/sharpness", methods=["POST"])
    def api_job_sharpness():
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")

        runner = app._job_runner

        def work(job):
            from sharpness import score_collection_photos

            thread_db = Database(db_path)
            job["_start_time"] = time.time()

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

            # Save scores to database
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
            # Don't return the full results list (could be huge)
            del result["results"]
            return result

        job_id = runner.start(
            "sharpness",
            work,
            config={
                "collection_id": collection_id,
            },
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs/classify", methods=["POST"])
    def api_job_classify():
        import config as cfg

        user_cfg = cfg.load()
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        labels_file = body.get("labels_file")
        model_id = body.get("model_id")
        model_name = body.get("model_name", "bioclip")
        threshold = body.get("threshold", user_cfg["classification_threshold"])
        grouping_window = body.get(
            "grouping_window", user_cfg["grouping_window_seconds"]
        )
        similarity_threshold = body.get(
            "similarity_threshold", user_cfg.get("similarity_threshold", 0.85)
        )
        reclassify = body.get("reclassify", False)

        if not collection_id:
            return jsonify({"error": "collection_id required"}), 400

        runner = app._job_runner

        def work(job):
            import tempfile
            from datetime import datetime as dt

            from PIL import Image
            from classifier import Classifier
            from compare import categorize, read_xmp_keywords
            from grouping import (
                consensus_prediction,
                group_by_timestamp,
                refine_groups_by_similarity,
            )
            from image_loader import load_image

            thread_db = Database(db_path)
            job["_start_time"] = time.time()

            # Resolve model from registry
            from models import get_active_model, get_models

            if model_id:
                all_models = get_models()
                active_model = next(
                    (m for m in all_models if m["id"] == model_id and m["downloaded"]),
                    None,
                )
                if not active_model:
                    raise RuntimeError(
                        f"Model '{model_id}' not found or not downloaded."
                    )
            else:
                active_model = get_active_model()
            if not active_model:
                raise RuntimeError("No model available. Download one in Settings.")

            model_str = active_model["model_str"]
            weights_path = active_model["weights_path"]
            effective_name = active_model["name"]

            # Phase 1: Load taxonomy
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": "Loading taxonomy...",
                    "rate": 0,
                    "phase": "Step 1/5: Loading taxonomy",
                },
            )
            taxonomy_path = os.path.join(os.path.dirname(__file__), "taxonomy.json")
            tax = None
            if os.path.exists(taxonomy_path):
                from taxonomy import Taxonomy

                tax = Taxonomy(taxonomy_path)

            # Phase 2: Load labels
            labels = None
            if labels_file and os.path.exists(labels_file):
                with open(labels_file) as f:
                    labels = [line.strip() for line in f if line.strip()]
                log.info("Using %d labels from file: %s", len(labels), labels_file)
            else:
                # Try active labels from the labels manager
                from labels import get_active_labels

                active_labels = get_active_labels()
                if active_labels and os.path.exists(
                    active_labels.get("labels_file", "")
                ):
                    with open(active_labels["labels_file"]) as f:
                        labels = [line.strip() for line in f if line.strip()]
                    log.info(
                        "Using %d labels from: %s",
                        len(labels),
                        active_labels.get("name", active_labels["labels_file"]),
                    )

            use_tol = False
            if not labels:
                log.info(
                    "No regional labels available — using Tree of Life classifier (all species)"
                )
                use_tol = True

            # Phase 3: Get photos from collection
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": 0,
                    "current_file": "Loading collection photos...",
                    "rate": 0,
                    "phase": "Step 2/5: Loading photos",
                },
            )
            photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(photos)
            job["progress"]["total"] = total

            log.info(
                "Classifying %d photos with '%s' (%s)", total, effective_name, model_str
            )

            # Clear existing predictions if re-classifying
            if reclassify:
                photo_ids = [p["id"] for p in photos]
                thread_db.clear_predictions(collection_photo_ids=photo_ids)
                log.info(
                    "Cleared existing predictions for %d photos (re-classify)",
                    len(photo_ids),
                )

            # Phase 4: Initialize classifier
            if use_tol:
                phase_msg = f"Loading {effective_name} Tree of Life classifier..."
            else:
                phase_msg = (
                    f"Loading {effective_name} model and computing label embeddings..."
                )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": total,
                    "current_file": phase_msg,
                    "rate": 0,
                    "phase": "Step 3/5: Loading model",
                },
            )

            def _emb_progress(current, emb_total):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": emb_total,
                        "current_file": f"Computing label embeddings ({current}/{emb_total})...",
                        "rate": 0,
                        "phase": "Step 3/5: Computing embeddings",
                    },
                )

            clf = Classifier(
                labels=None if use_tol else labels,
                model_str=model_str,
                pretrained_str=weights_path,
                embedding_progress_callback=_emb_progress,
            )

            # Phase 5: Detect subjects (MegaDetector)
            # Run detection first so we can crop to subject for better classification
            detected = 0
            detection_map = {}  # photo_id -> primary detection
            try:
                from detector import detect_animals, get_primary_detection

                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": total,
                        "current_file": "Loading MegaDetector...",
                        "rate": 0,
                        "phase": "Step 4/5: Detecting subjects",
                    },
                )

                for i, photo in enumerate(photos):
                    folder_path = folders.get(photo["folder_id"], "")
                    image_path = os.path.join(folder_path, photo["filename"])

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
                            "phase": "Step 4/5: Detecting subjects",
                        },
                    )

                    detections = detect_animals(image_path)
                    primary = get_primary_detection(detections)

                    if primary:
                        detected += 1
                        detection_map[photo["id"]] = primary

                        # Store detection in DB
                        from sharpness import compute_sharpness

                        det_box = primary["box"]
                        det_conf = primary["confidence"]
                        subject_size = det_box["w"] * det_box["h"]

                        overall_sharpness = compute_sharpness(image_path)
                        subject_sharpness = None
                        quality = 0

                        try:
                            img = Image.open(image_path)
                            iw, ih = img.size
                            px = int(det_box["x"] * iw)
                            py = int(det_box["y"] * ih)
                            pw = int(det_box["w"] * iw)
                            ph = int(det_box["h"] * ih)
                            subject_sharpness = compute_sharpness(
                                image_path, region=(px, py, pw, ph)
                            )
                        except Exception:
                            subject_sharpness = overall_sharpness

                        if subject_sharpness is not None and subject_size is not None:
                            import math

                            norm_sharp = min(1.0, math.log1p(subject_sharpness) / 10.0)
                            norm_size = min(1.0, subject_size * 4)
                            quality = round(0.7 * norm_sharp + 0.3 * norm_size, 4)

                        thread_db.update_photo_quality(
                            photo["id"],
                            detection_box=det_box,
                            detection_conf=det_conf,
                            subject_sharpness=subject_sharpness,
                            subject_size=subject_size,
                            quality_score=quality,
                            sharpness=overall_sharpness,
                        )

                log.info(
                    "Detection done: %d animals detected out of %d photos",
                    detected,
                    total,
                )
            except (ImportError, RuntimeError) as e:
                if "PytorchWildlife" in str(e):
                    log.info(
                        "PytorchWildlife not installed — skipping detection (classifying full images)"
                    )
                else:
                    log.warning(
                        "Detection unavailable: %s — classifying full images", e
                    )
            except Exception:
                log.warning(
                    "Detection failed (non-fatal) — classifying full images",
                    exc_info=True,
                )

            # Phase 6: Classify each photo (cropped to subject when available)
            # Skip photos that already have predictions (unless re-classifying)
            existing_preds = set()
            if not reclassify:
                rows = thread_db.conn.execute(
                    "SELECT DISTINCT photo_id FROM predictions WHERE model = ?",
                    (effective_name,),
                ).fetchall()
                existing_preds = {r["photo_id"] for r in rows}

            raw_results = []
            failed = 0
            skipped_existing = 0
            job["_start_time"] = (
                time.time()
            )  # reset rate timer for classification phase

            for i, photo in enumerate(photos):
                folder_path = folders.get(photo["folder_id"], "")
                image_path = os.path.join(folder_path, photo["filename"])

                job["progress"]["current"] = i + 1
                job["progress"]["current_file"] = photo["filename"]
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
                        "phase": "Step 5/5: Classifying species",
                    },
                )

                if photo["id"] in existing_preds:
                    skipped_existing += 1
                    continue

                img = load_image(image_path, max_size=None)
                if img is None:
                    failed += 1
                    continue

                # Crop to detected subject with padding for better classification
                primary = detection_map.get(photo["id"])
                if primary:
                    iw, ih = img.size
                    box = primary["box"]
                    # Add 20% padding around the detection box
                    pad_w = box["w"] * 0.2
                    pad_h = box["h"] * 0.2
                    x1 = max(0, int((box["x"] - pad_w) * iw))
                    y1 = max(0, int((box["y"] - pad_h) * ih))
                    x2 = min(iw, int((box["x"] + box["w"] + pad_w) * iw))
                    y2 = min(ih, int((box["y"] + box["h"] + pad_h) * ih))
                    crop = img.crop((x1, y1, x2, y2))
                    # Only use crop if it's reasonably sized
                    if crop.size[0] >= 50 and crop.size[1] >= 50:
                        img = crop

                # Resize for model input
                img.thumbnail((1024, 1024), Image.LANCZOS)

                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    tmp_path = tmp.name
                    img.save(tmp_path, quality=85)

                try:
                    all_preds, embedding = clf.classify_with_embedding(
                        tmp_path, threshold=0
                    )
                except Exception:
                    log.warning(
                        "Classification failed for %s", photo["filename"], exc_info=True
                    )
                    failed += 1
                    continue
                finally:
                    os.unlink(tmp_path)

                # Store embedding in DB for grouping
                if embedding is not None:
                    thread_db.conn.execute(
                        "UPDATE photos SET embedding = ? WHERE id = ?",
                        (embedding.tobytes(), photo["id"]),
                    )

                if not all_preds:
                    continue

                top_pred = all_preds[0]
                preds = [p for p in all_preds if p["score"] >= threshold]

                if not preds:
                    log.info(
                        '%s: "%s" at %.0f%% (below %.0f%% threshold — skipped)',
                        photo["filename"],
                        top_pred["species"],
                        top_pred["score"] * 100,
                        threshold * 100,
                    )
                    continue

                top = preds[0]
                log.info(
                    '%s: "%s" at %.0f%%',
                    photo["filename"],
                    top["species"],
                    top["score"] * 100,
                )

                # Parse timestamp for grouping
                timestamp = None
                if photo["timestamp"]:
                    try:
                        timestamp = dt.fromisoformat(photo["timestamp"])
                    except Exception:
                        pass

                raw_results.append(
                    {
                        "photo": photo,
                        "folder_path": folder_path,
                        "image_path": image_path,
                        "prediction": top["species"],
                        "confidence": top["score"],
                        "timestamp": timestamp,
                        "filename": photo["filename"],
                        "embedding": embedding,
                        "taxonomy": top.get("taxonomy"),
                    }
                )

            # Phase 7: Group by timestamp and compute consensus
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": total,
                    "total": total,
                    "current_file": "Grouping bursts and computing consensus...",
                    "rate": 0,
                    "phase": "Finalizing results",
                },
            )

            groups = group_by_timestamp(raw_results, window_seconds=grouping_window)
            groups = refine_groups_by_similarity(
                groups, similarity_threshold=similarity_threshold
            )
            predictions_stored = 0
            group_count = 0
            skipped_match = 0

            for group in groups:
                if len(group) == 1:
                    # Single photo — store directly
                    item = group[0]
                    photo = item["photo"]
                    folder_path = item["folder_path"]

                    category = "new"
                    if tax:
                        xmp_path = os.path.join(
                            folder_path, os.path.splitext(photo["filename"])[0] + ".xmp"
                        )
                        existing = read_xmp_keywords(xmp_path)
                        category = categorize(item["prediction"], existing, tax)

                    if category == "match":
                        skipped_match += 1
                        continue

                    tax_hierarchy = item.get("taxonomy") or (
                        tax.get_hierarchy(item["prediction"]) if tax else {}
                    )
                    thread_db.add_prediction(
                        photo_id=photo["id"],
                        species=item["prediction"],
                        confidence=round(item["confidence"], 4),
                        model=model_name,
                        category=category,
                        taxonomy=tax_hierarchy,
                    )
                    predictions_stored += 1
                else:
                    # Group — compute consensus
                    group_count += 1
                    gid = f"g{group_count:04d}"
                    cons_input = [
                        {
                            "prediction": item["prediction"],
                            "confidence": item["confidence"],
                        }
                        for item in group
                    ]
                    cons = consensus_prediction(cons_input)
                    if not cons:
                        continue

                    # Categorize using the consensus prediction
                    representative = group[0]
                    category = "new"
                    if tax:
                        xmp_path = os.path.join(
                            representative["folder_path"],
                            os.path.splitext(representative["photo"]["filename"])[0]
                            + ".xmp",
                        )
                        existing = read_xmp_keywords(xmp_path)
                        category = categorize(cons["prediction"], existing, tax)

                    if category == "match":
                        skipped_match += len(group)
                        continue

                    individual_json = json.dumps(cons["individual_predictions"])
                    # Use taxonomy from the representative prediction, or look up from taxonomy
                    rep_tax = group[0].get("taxonomy")
                    cons_hierarchy = rep_tax or (
                        tax.get_hierarchy(cons["prediction"]) if tax else {}
                    )

                    # Store prediction for each photo in the group
                    for item in group:
                        thread_db.add_prediction(
                            photo_id=item["photo"]["id"],
                            species=cons["prediction"],
                            confidence=round(cons["confidence"], 4),
                            model=model_name,
                            category=category,
                            group_id=gid,
                            vote_count=cons["vote_count"],
                            total_votes=cons["total_votes"],
                            individual=individual_json,
                            taxonomy=cons_hierarchy,
                        )
                    predictions_stored += len(group)

            below_threshold = total - len(raw_results) - failed - skipped_existing
            singles = len([g for g in groups if len(g) == 1])
            grouped_photos = sum(len(g) for g in groups if len(g) > 1)
            log.info(
                "Classification complete: %d photos processed, %d predictions stored "
                "(%d singles, %d in %d burst groups), %d already classified, "
                "%d already labeled, %d below threshold, %d failed",
                total,
                predictions_stored,
                singles,
                grouped_photos,
                group_count,
                skipped_existing,
                skipped_match,
                below_threshold,
                failed,
            )

            return {
                "total": total,
                "predictions_stored": predictions_stored,
                "burst_groups": group_count,
                "already_classified": skipped_existing,
                "already_labeled": skipped_match,
                "below_threshold": below_threshold,
                "detected": detected,
                "failed": failed,
            }

        job_id = runner.start(
            "classify",
            work,
            config={
                "collection_id": collection_id,
                "model_name": model_name,
            },
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/jobs")
    def api_jobs_list():
        runner = app._job_runner
        db = _get_db()
        active = runner.list_jobs()
        history = runner.get_history(db, limit=10)
        return jsonify({"active": active, "history": history})

    @app.route("/api/jobs/<job_id>")
    def api_job_status(job_id):
        job = app._job_runner.get(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404
        return jsonify(job)

    @app.route("/api/jobs/<job_id>/stream")
    def api_job_stream(job_id):
        """SSE stream of job progress events."""
        runner = app._job_runner
        job = runner.get(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404

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
                        if j and j["status"] in ("completed", "failed"):
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

        Auto-closes after 30s of inactivity to prevent stale connections
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
                        # Close after ~30s idle to free the thread
                        if idle_count >= 15:
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

    @app.route("/thumbnails/<filename>")
    def serve_thumbnail(filename):
        return send_from_directory(app.config["THUMB_CACHE_DIR"], filename)

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
            """SELECT pr.photo_id, p.embedding, p.filename, p.thumb_path,
                      pr.confidence, pr.taxonomy_order, pr.taxonomy_family
               FROM predictions pr
               JOIN photos p ON p.id = pr.photo_id
               WHERE pr.species = ? AND p.embedding IS NOT NULL""",
            (species_name,),
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
        dist_matrix = 1.0 - sim_matrix
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
            return jsonify({"error": "photo_ids and label required"}), 400

        kid = db.add_keyword(label)
        for pid in photo_ids:
            db.tag_photo(pid, kid)
            db.queue_change(pid, "keyword_add", label)

        log.info("Labeled %d photos as '%s'", len(photo_ids), label)
        return jsonify({"ok": True, "updated": len(photo_ids), "keyword_id": kid})

    @app.route("/api/species")
    def api_species_list():
        """List all species with prediction counts, for the variant explorer."""
        db = _get_db()
        rows = db.conn.execute(
            """SELECT species, COUNT(*) as photo_count,
                      taxonomy_order, taxonomy_family, taxonomy_genus,
                      scientific_name
               FROM predictions
               WHERE status != 'rejected'
               GROUP BY species
               ORDER BY photo_count DESC"""
        ).fetchall()
        return jsonify([dict(r) for r in rows])

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
            f"SELECT id, embedding FROM photos WHERE embedding IS NOT NULL AND id != ?",
            (photo_id,),
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

        results = []
        for idx in top_indices:
            pid = photo_ids[idx]
            sim = float(similarities[idx])
            photo = db.get_photo(pid)
            if photo:
                results.append(
                    {
                        "photo": dict(photo),
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
            return jsonify({"error": "Photo not found"}), 404

        result = dict(photo)
        # Remove binary embedding from response
        result.pop("embedding", None)

        # Parse detection_box from JSON string
        if result.get("detection_box") and isinstance(result["detection_box"], str):
            result["detection_box"] = json.loads(result["detection_box"])

        # Get predictions for this photo
        preds = db.conn.execute(
            """SELECT species, confidence, model, category, status,
                      group_id, vote_count, total_votes, individual
               FROM predictions WHERE photo_id = ?
               ORDER BY confidence DESC""",
            (photo_id,),
        ).fetchall()
        result["predictions"] = [dict(p) for p in preds]

        # Get keywords
        keywords = db.get_photo_keywords(photo_id)
        result["keywords"] = [dict(k) for k in keywords]

        # Compute crop info if detection exists
        if result.get("detection_box"):
            box = result["detection_box"]
            pad = 0.2
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
        from PIL import Image
        from image_loader import load_image

        db = _get_db()
        photo = db.conn.execute(
            "SELECT p.filename, p.detection_box, f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
            (photo_id,),
        ).fetchone()
        if not photo:
            return "Not found", 404

        image_path = os.path.join(photo["path"], photo["filename"])
        img = load_image(image_path, max_size=None)
        if img is None:
            return "Could not load image", 500

        det_box = photo["detection_box"]
        if det_box:
            if isinstance(det_box, str):
                det_box = json.loads(det_box)
            iw, ih = img.size
            pad_w = det_box["w"] * 0.2
            pad_h = det_box["h"] * 0.2
            x1 = max(0, int((det_box["x"] - pad_w) * iw))
            y1 = max(0, int((det_box["y"] - pad_h) * ih))
            x2 = min(iw, int((det_box["x"] + det_box["w"] + pad_w) * iw))
            y2 = min(ih, int((det_box["y"] + det_box["h"] + pad_h) * ih))
            crop = img.crop((x1, y1, x2, y2))
            if crop.size[0] >= 50 and crop.size[1] >= 50:
                img = crop

        img.thumbnail((800, 800), Image.LANCZOS)
        import io

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
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
        img.save(cache_path, format="JPEG", quality=90)
        return send_file(cache_path, mimetype="image/jpeg")

    # -- Logs page --

    @app.route("/logs")
    def logs_page():
        return render_template("logs.html")

    @app.route("/stats")
    def stats_page():
        return render_template("stats.html")

    return app


def main():
    parser = argparse.ArgumentParser(description="Spotter Photo Browser")
    parser.add_argument(
        "--db",
        default=os.path.expanduser("~/.spotter/spotter.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--thumb-dir",
        default=os.path.expanduser("~/.spotter/thumbnails"),
        help="Path to thumbnail cache directory",
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    app = create_app(db_path=args.db, thumb_cache_dir=args.thumb_dir)

    # Open browser after server is ready, not before
    if not args.no_browser:
        import threading
        import urllib.request

        def _open_browser():
            url = f"http://localhost:{args.port}"
            for _ in range(50):  # try for up to 5 seconds
                try:
                    urllib.request.urlopen(url, timeout=0.1)
                    webbrowser.open(url)
                    return
                except Exception:
                    time.sleep(0.1)

        threading.Thread(target=_open_browser, daemon=True).start()

    app.run(host="127.0.0.1", port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
