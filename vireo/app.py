"""Flask web app for the Vireo photo browser.

Usage:
    python vireo/app.py --db ~/.vireo/vireo.db [--port 8080]
"""

import argparse
import json
import logging
import logging.handlers
import math
import os
import queue
import subprocess
import sys
import time
import webbrowser
from datetime import UTC
from pathlib import Path
from urllib.parse import quote

import places
from db import KEYWORD_TYPES, Database
from flask import (
    Flask,
    Response,
    abort,
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
from preview_cache import evict_if_over_quota as evict_preview_cache_if_over_quota

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# File logging is attached only when the server actually starts (see
# main() / _setup_file_logging). Importing this module — e.g. from pytest
# fixtures — must NOT touch ~/.vireo/vireo.log, or test tracebacks end up
# in the user's real log file.
def _setup_file_logging(log_dir=None):
    root = logging.getLogger()
    if any(getattr(h, "_vireo_file_handler", False) for h in root.handlers):
        return
    if log_dir is None:
        log_dir = os.path.expanduser("~/.vireo")
    os.makedirs(log_dir, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "vireo.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    handler._vireo_file_handler = True
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)


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


# Maximum number of bound parameters per SQL statement. SQLite's
# ``SQLITE_MAX_VARIABLE_NUMBER`` defaults to 32766 on builds since 3.32 but
# remains 999 on older builds (and on some packagers' default builds). Bulk
# duplicate-cleanup actions can hand us thousands of photo ids at once, so
# we chunk every IN-clause query under this cap to stay portable across
# SQLite versions. Sized below 999 to leave headroom for additional bound
# parameters in joined statements.
_SQL_PARAM_CHUNK = 900


def _chunked(seq, size=_SQL_PARAM_CHUNK):
    """Yield ``seq`` in successive lists of at most ``size`` items."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


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


def _compute_time_range(photos_by_id, photo_ids):
    """Return [min_ts, max_ts] ISO strings for photo_ids, or [None, None]."""
    timestamps = [
        photos_by_id[pid]["timestamp"]
        for pid in photo_ids
        if pid in photos_by_id and photos_by_id[pid].get("timestamp")
    ]
    if not timestamps:
        return [None, None]
    return [min(timestamps), max(timestamps)]


def _find_merge_target(encounters, detached_range, target_species):
    """Find an encounter index whose confirmed species matches target_species and
    whose time range is adjacent to detached_range (no other encounter sits in
    the gap between them). Returns None if none found.
    """
    d_min, d_max = detached_range
    if d_min is None or d_max is None:
        return None

    other_ranges = []
    for i, e in enumerate(encounters):
        tr = e.get("time_range") or [None, None]
        if tr[0] is not None and tr[1] is not None:
            other_ranges.append((i, tr[0], tr[1]))

    for i, e in enumerate(encounters):
        if not e.get("species_confirmed"):
            continue
        if e.get("confirmed_species") != target_species:
            continue
        tr = e.get("time_range") or [None, None]
        if tr[0] is None or tr[1] is None:
            continue
        c_min, c_max = tr[0], tr[1]
        if c_max < d_min:
            gap_start, gap_end = c_max, d_min
        elif d_max < c_min:
            gap_start, gap_end = d_max, c_min
        else:
            return i  # overlapping — treat as adjacent
        intervening = False
        for j, o_min, o_max in other_ranges:
            if j == i:
                continue
            if o_max > gap_start and o_min < gap_end:
                intervening = True
                break
        if not intervening:
            return i
    return None


def _auto_detach_burst_for_species(results, enc_idx, burst_idx, new_species):
    """Detach the burst at (enc_idx, burst_idx) from its encounter. If an adjacent
    encounter already has new_species as its confirmed species, merge the burst
    into that encounter; otherwise create a new single-burst encounter with
    new_species confirmed. Mutates results in place.
    """
    from pipeline import rebuild_species_predictions

    encounters = results["encounters"]
    enc = encounters[enc_idx]
    bursts = enc["bursts"]
    detached = bursts.pop(burst_idx)
    detached_ids = detached["photo_ids"]

    photos_by_id = {p["id"]: p for p in results.get("photos", [])}
    detached_range = _compute_time_range(photos_by_id, detached_ids)

    if len(bursts) == 0:
        encounters.pop(enc_idx)
    else:
        remaining = [pid for pid in enc["photo_ids"] if pid not in set(detached_ids)]
        enc["photo_ids"] = remaining
        enc["photo_count"] = len(remaining)
        enc["burst_count"] = len(bursts)
        enc["species_predictions"] = rebuild_species_predictions(results, remaining)
        for b in bursts:
            b["species_predictions"] = rebuild_species_predictions(results, b["photo_ids"])
        enc["time_range"] = _compute_time_range(photos_by_id, remaining)

    detached["species_predictions"] = rebuild_species_predictions(results, detached_ids)

    merge_idx = _find_merge_target(encounters, detached_range, new_species)
    if merge_idx is not None:
        target = encounters[merge_idx]
        target["bursts"].append(detached)
        target["photo_ids"] = list(target["photo_ids"]) + list(detached_ids)
        target["photo_count"] = len(target["photo_ids"])
        target["burst_count"] = len(target["bursts"])
        target["species_predictions"] = rebuild_species_predictions(
            results, target["photo_ids"]
        )
        t_min, t_max = target.get("time_range") or [None, None]
        d_min, d_max = detached_range
        mins = [x for x in (t_min, d_min) if x is not None]
        maxs = [x for x in (t_max, d_max) if x is not None]
        target["time_range"] = [
            min(mins) if mins else None,
            max(maxs) if maxs else None,
        ]
    else:
        encounters.append({
            "species": enc.get("species"),
            "confirmed_species": new_species,
            "species_predictions": detached["species_predictions"],
            "species_confirmed": True,
            "photo_count": len(detached_ids),
            "burst_count": 1,
            "time_range": detached_range,
            "photo_ids": list(detached_ids),
            "bursts": [detached],
        })

    summary = results.setdefault("summary", {})
    summary["encounter_count"] = len(encounters)
    summary["burst_count"] = sum(e.get("burst_count", 0) for e in encounters)


# The canonical implementation lives in ``new_images.py`` so non-Flask
# modules (e.g. ``pipeline_job.py``) can import it without pulling in the
# app module. Kept aliased here under the original private name for
# backward-compatibility with existing call sites and tests.
from new_images import invalidate_new_images_after_scan as _invalidate_new_images_after_scan  # noqa: E402


def _migrate_legacy_preview_cache(app):
    """One-shot migration of pre-refactor preview cache files.

    Two classes of pre-existing files are made visible to the LRU here:

    1. Unsized {id}.jpg from the old /full endpoint. These are renamed to
       {id}_<preview_max_size>.jpg and tracked.
    2. Sized {id}_{size}.jpg files written by an earlier /preview before
       preview_cache existed. These already match the new naming scheme,
       so we just insert a tracking row pointing at the file in place.

    Both classes were previously invisible to accounting and eviction —
    they sat on disk indefinitely unless the user hit Clear Cache. Runs
    once per process start; a no-op when nothing needs adopting.

    If preview_max_size=0 (meaning "full") we can't assign a size tier
    to unsized {id}.jpg, so those are left in place for Clear Cache to
    remove later. Sized files are still adopted in that case.
    """
    import re

    import config as cfg

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    if not os.path.isdir(preview_dir):
        return

    unsized_pat = re.compile(r"^(\d+)\.jpg$")
    sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
    try:
        all_files = os.listdir(preview_dir)
    except OSError:
        return
    unsized_files = [f for f in all_files if unsized_pat.match(f)]
    sized_files = [f for f in all_files if sized_pat.match(f)]
    if not unsized_files and not sized_files:
        return

    # Read preview_max_size explicitly so a configured 0 ("full res")
    # stays 0 and the tier-assignment guard below is reachable.
    raw_size = cfg.load().get("preview_max_size")
    target_size = 0 if raw_size == 0 else int(raw_size or 1920)

    db = Database(app.config["DB_PATH"])
    try:
        migrated = 0
        orphaned = 0
        adopted = 0
        if unsized_files and target_size == 0:
            log.info(
                "Leaving %d legacy preview files (preview_max_size=0 — can't assign tier)",
                len(unsized_files),
            )

        # Pass 1: rename unsized {id}.jpg → {id}_<target>.jpg + insert.
        if target_size:
            for fname in unsized_files:
                m = unsized_pat.match(fname)
                photo_id = int(m.group(1))
                src = os.path.join(preview_dir, fname)
                dst = os.path.join(preview_dir, f"{photo_id}_{target_size}.jpg")
                # Skip orphans: if the photo was deleted, inserting into
                # preview_cache would raise a FK error and rolling back the
                # already-performed os.rename is ugly. Unlink the orphan so
                # disk doesn't keep pointing at vanished photos.
                photo_row = db.conn.execute(
                    "SELECT 1 FROM photos WHERE id=?", (photo_id,)
                ).fetchone()
                if photo_row is None:
                    try:
                        os.remove(src)
                        orphaned += 1
                    except OSError:
                        pass
                    continue
                if os.path.exists(dst):
                    try:
                        os.remove(src)
                    except OSError:
                        pass
                    continue
                try:
                    os.rename(src, dst)
                    st = os.stat(dst)
                    db.preview_cache_insert(photo_id, target_size, st.st_size)
                    migrated += 1
                except OSError as e:
                    log.warning("Failed to migrate legacy preview %s: %s", src, e)

        # Pass 2: adopt pre-existing sized {id}_{size}.jpg files that
        # aren't tracked yet. These are produced by older /preview calls
        # that ran before preview_cache existed; without this pass they
        # stay invisible to accounting/eviction even though they already
        # match the new naming scheme.
        for fname in sized_files:
            m = sized_pat.match(fname)
            photo_id = int(m.group(1))
            size = int(m.group(2))
            path = os.path.join(preview_dir, fname)
            if db.preview_cache_get(photo_id, size):
                continue
            photo_row = db.conn.execute(
                "SELECT 1 FROM photos WHERE id=?", (photo_id,)
            ).fetchone()
            if photo_row is None:
                try:
                    os.remove(path)
                    orphaned += 1
                except OSError:
                    pass
                continue
            try:
                st = os.stat(path)
                db.preview_cache_insert(photo_id, size, st.st_size)
                adopted += 1
            except OSError as e:
                log.warning("Failed to adopt sized preview %s: %s", path, e)

        if migrated:
            log.info(
                "Migrated %d legacy preview cache files to size %d",
                migrated, target_size,
            )
        if adopted:
            log.info("Adopted %d untracked sized preview files into LRU", adopted)
        if orphaned:
            log.info("Removed %d orphaned legacy preview files", orphaned)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _enforce_preview_cache_quota_at_startup(app):
    """Run one eviction pass at startup so migration / prior runs can't
    leave the app over quota indefinitely.
    """
    from preview_cache import evict_if_over_quota

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    db = Database(app.config["DB_PATH"])
    try:
        evict_if_over_quota(db, vireo_dir)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def create_app(db_path, thumb_cache_dir=None, api_token=None):
    """Create the Flask app for the Vireo photo browser.

    Args:
        db_path: path to the SQLite database
        thumb_cache_dir: path to thumbnail cache directory
        api_token: optional token required on /api/v1/* requests via the
            ``X-Vireo-Token`` header. When ``None`` (default), all /api/v1/*
            traffic is rejected with 401 — the token is expected to be
            supplied by ``main()`` after calling ``runtime.generate_token``.
    """
    app = Flask(
        __name__, template_folder=os.path.join(os.path.dirname(__file__), "templates")
    )
    app.config["DB_PATH"] = db_path
    app.config["THUMB_CACHE_DIR"] = thumb_cache_dir or os.path.expanduser(
        "~/.vireo/thumbnails"
    )
    app.config["API_TOKEN"] = api_token

    _migrate_legacy_preview_cache(app)
    _enforce_preview_cache_quota_at_startup(app)

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

    def _cleanup_cached_files_for_deleted_photos(files):
        """Remove thumbnail, preview, and working-copy files for deleted photos.

        ``files`` is the list returned by ``db.delete_photos`` /
        ``db.delete_folder``. The FK cascade drops preview_cache rows when
        photos are deleted, but the on-disk files stay unless we unlink
        them here — otherwise they leak into untracked bytes that eviction
        can't see.

        Note: if an unlink fails (e.g. file locked on Windows), the file
        remains on disk as an orphan because the cascade has already removed
        the preview_cache row. "Clear cache" in Settings recovers by
        globbing the directory.
        """
        import glob as _glob
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_dir)
        preview_dir = os.path.join(vireo_dir, "previews")
        working_dir = os.path.join(vireo_dir, "working")
        for f in files:
            pid = f["photo_id"]
            # {id}.jpg lives in all three dirs (legacy full preview, thumb,
            # working copy). {id}_{size}.jpg is sized preview variants.
            for d in [thumb_dir, preview_dir, working_dir]:
                cached = os.path.join(d, f"{pid}.jpg")
                if os.path.isfile(cached):
                    try:
                        os.remove(cached)
                    except OSError as e:
                        log.warning(
                            "Failed to remove cached file %s after photo "
                            "delete — will be reclaimed by Clear Cache: %s",
                            cached, e,
                        )
            for variant in _glob.glob(os.path.join(preview_dir, f"{pid}_*.jpg")):
                try:
                    os.remove(variant)
                except OSError as e:
                    log.warning(
                        "Failed to remove preview variant %s after photo "
                        "delete — will be reclaimed by Clear Cache: %s",
                        variant, e,
                    )

    @app.before_request
    def _enforce_api_v1_token():
        if not request.path.startswith("/api/v1/"):
            return None
        expected = app.config.get("API_TOKEN")
        if not expected:
            # No token configured → deny all v1 traffic.
            return json_error("API token not configured", 401)
        if request.headers.get("X-Vireo-Token") != expected:
            return json_error("Invalid or missing X-Vireo-Token", 401)
        return None

    @app.route("/api/health")
    def api_health():
        return jsonify({"status": "ok"})

    @app.route("/api/v1/health")
    def api_v1_health():
        # The "service" field is a Vireo-specific marker the single-instance
        # guard's probe checks for. An unrelated local service that happens
        # to return 200 on /api/v1/health (catch-all) would not carry it,
        # so Vireo can distinguish a live peer from a port-reusing stranger.
        from runtime import SERVICE_MARKER
        return jsonify({"service": SERVICE_MARKER, "status": "ok"})

    @app.route("/api/v1/version")
    def api_v1_version():
        return api_version()  # reuse existing implementation

    @app.route("/api/v1/shutdown", methods=["POST"])
    def api_v1_shutdown():
        import signal
        import threading

        def _shutdown():
            os.kill(os.getpid(), signal.SIGTERM)

        threading.Timer(0.5, _shutdown).start()
        return jsonify({"status": "shutting_down"})

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
    # Migrate the legacy 'Needs Classification' default collection BEFORE
    # seeding defaults — otherwise create_default_collections inserts
    # 'Needs Identification' first, then the migration skips renaming
    # because the target name already exists, leaving a duplicate.
    init_db.migrate_default_subject_collection()
    init_db.create_default_collections()

    # Wildlife backfill timing:
    # - Subsequent boots: marker is set, nothing to do, fast.
    # - First boot after upgrade (marker unset): run species marking +
    #   backfill SYNCHRONOUSLY before accepting requests. Otherwise a
    #   user could remove Wildlife from a species-tagged photo during
    #   the few-second window before the background thread completes,
    #   and the backfill would silently re-add it (clobbering user
    #   intent before the marker gets set).
    # - mark_species runs in the background regardless, to catch new
    #   species keywords added between boots. The backfill won't re-run
    #   from the background path (marker check inside backfill).
    import threading

    from db import Database as _Database  # avoid shadowing in nested fn

    _WILDLIFE_BACKFILL_DONE_KEY = _Database._WILDLIFE_BACKFILL_DONE_KEY

    def _mark_species_and_maybe_backfill(db, log_label):
        """Load taxonomy, mark species keywords, and run the one-shot
        Wildlife backfill. Returns silently on any failure so callers
        can choose between sync (block startup) and async (log only)."""
        from taxonomy import load_local_taxonomy

        tax = load_local_taxonomy()
        if tax is None:
            # No taxonomy yet — leave the marker unset so the backfill
            # retries on a future boot once taxonomy is downloaded.
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
        except Exception:
            log.debug("[%s] mark_species_keywords failed", log_label, exc_info=True)
            return
        try:
            db.backfill_wildlife_genre()
        except Exception:
            log.debug("[%s] backfill_wildlife_genre failed", log_label, exc_info=True)

    if init_db.get_meta(_WILDLIFE_BACKFILL_DONE_KEY) != "1":
        # First boot after upgrade. Block startup until the one-shot
        # backfill completes so concurrent user edits in that window
        # can't be overwritten.
        log.info("Wildlife backfill marker unset; running species marking "
                 "+ backfill synchronously before serving requests")
        _t1 = time.time()
        _mark_species_and_maybe_backfill(init_db, "sync-startup")
        log.info("Synchronous species marking + backfill took %.2fs",
                 time.time() - _t1)

    def _mark_species():
        try:
            bg_db = Database(db_path)
        except Exception:
            log.debug("Could not open background db for species marking", exc_info=True)
            return
        _mark_species_and_maybe_backfill(bg_db, "background")

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

    # Self-healing background backfill of missing working copies. RAW (and
    # oversized JPEG) imports need a JPEG working copy at
    # ``~/.vireo/working/{photo_id}.jpg`` so /photos/<id>/original can serve
    # a static file without on-demand RAW decode (5-7s per file). The
    # scan() path already extracts these inline for newly-discovered photos,
    # but legacy rows (imported before the feature, or with a previous
    # extraction failure that has since been fixed) carry NULL
    # working_copy_path forever without this pass.
    #
    # Surfaced as an ephemeral JobRunner job so the bottom panel shows
    # progress, but never written to job_history (it runs every startup —
    # persisting would be noise). Skipped entirely when a fast EXISTS check
    # confirms no candidates remain, so steady-state restarts pay nothing.
    def _kickoff_working_copy_backfill():
        from scanner import (
            backfill_working_copies,
            working_copy_backfill_candidate_count,
        )

        try:
            wcdb = Database(db_path)
            # Defer to scanner's own predicate so the gate matches what
            # ``_extract_working_copies`` will actually process. A naive
            # ``working_copy_path IS NULL`` check would also fire on
            # libraries of only small JPEGs (which the extractor
            # intentionally skips), launching a no-op backfill on every
            # restart.
            candidate_count = working_copy_backfill_candidate_count(wcdb)
        except Exception:
            log.exception("Working-copy backfill: candidate check failed")
            return
        if candidate_count == 0:
            log.debug("Working-copy backfill: no candidates, skipping")
            return

        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        runner = app._job_runner

        def work(job):
            thread_db = Database(db_path)
            # Working-copy backfill is workspace-agnostic (photos are
            # global), but the JobRunner path leans on having an active
            # workspace for some bookkeeping. Mirror the active workspace
            # of init_db so any incidental ws-scoped helpers stay valid.
            active_ws = init_db._active_workspace_id
            if active_ws is not None:
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
                        "phase": f"{current:,} / {total:,} working copies",
                    },
                )

            def status_cb(message):
                runner.push_event(job["id"], "progress", {
                    "phase": message,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                })

            def cancel_check():
                return runner.is_cancelled(job["id"])

            return backfill_working_copies(
                thread_db, vireo_dir,
                progress_callback=progress_cb,
                status_callback=status_cb,
                cancel_check=cancel_check,
            )

        try:
            runner.start(
                "working_copy_backfill", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start working-copy backfill job")

    # Expose the kickoff so tests can drive it synchronously without
    # waiting on the Timer. Production wiring uses threading.Timer below;
    # tests call ``app._kickoff_working_copy_backfill()`` directly and
    # then block on the resulting JobRunner job.
    app._kickoff_working_copy_backfill = _kickoff_working_copy_backfill

    # Defer the kickoff slightly so the app finishes booting before the
    # first DB-heavy background pass starts churning. Mirrors the folder
    # health loop's grace period.
    #
    # Daemon=True so short-lived ``create_app`` callers (tests, scripts,
    # one-shot tooling) don't get pinned waiting on the 5-second delay
    # only to fire DB work after their real work is already done.
    _wc_backfill_timer = threading.Timer(5.0, _kickoff_working_copy_backfill)
    _wc_backfill_timer.daemon = True
    _wc_backfill_timer.start()

    # ----- thumb_path self-healing backfill -----
    # The dashboard's coverage card counts thumbnails by ``thumb_path IS NOT
    # NULL``, but for a long stretch the column was never populated by
    # production code, so libraries with 40k JPEGs cached on disk reported
    # "0 thumbnails" forever. This pass aligns the column with disk reality
    # for legacy rows, and clears it for photos whose cached file has since
    # been deleted (drift correction).
    #
    # Same shape as the working-copy backfill above: ephemeral JobRunner
    # job (so it shows in the bottom panel), never written to job_history,
    # skipped entirely when a fast count check finds nothing to do.
    def _kickoff_thumb_path_backfill():
        from thumbnails import (
            backfill_thumb_paths,
            thumb_path_backfill_candidate_count,
        )

        try:
            tpdb = Database(db_path)
            candidate_count = thumb_path_backfill_candidate_count(
                tpdb, app.config["THUMB_CACHE_DIR"],
            )
        except Exception:
            log.exception("thumb_path backfill: candidate check failed")
            return
        if candidate_count == 0:
            log.debug("thumb_path backfill: no candidates, skipping")
            return

        runner = app._job_runner
        cache_dir = app.config["THUMB_CACHE_DIR"]

        def work(job):
            thread_db = Database(db_path)
            active_ws = init_db._active_workspace_id
            if active_ws is not None:
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
                        "phase": f"{current:,} / {total:,} photos reconciled",
                    },
                )

            def status_cb(message):
                runner.push_event(job["id"], "progress", {
                    "phase": message,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                })

            def cancel_check():
                return runner.is_cancelled(job["id"])

            return backfill_thumb_paths(
                thread_db, cache_dir,
                progress_callback=progress_cb,
                status_callback=status_cb,
                cancel_check=cancel_check,
            )

        try:
            runner.start(
                "thumb_path_backfill", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start thumb_path backfill job")

    app._kickoff_thumb_path_backfill = _kickoff_thumb_path_backfill

    _thumb_backfill_timer = threading.Timer(6.0, _kickoff_thumb_path_backfill)
    _thumb_backfill_timer.daemon = True
    _thumb_backfill_timer.start()

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

    def _auto_open_tab(nav_id):
        """Best-effort: append nav_id to the active workspace's open_tabs.

        Called from openable page routes so direct URL visits / shortcuts
        keep the navbar consistent. Errors are swallowed (the page still renders).
        """
        try:
            _get_db().open_tab(nav_id)
        except Exception:
            log.exception("Failed to auto-open tab %r", nav_id)

    @app.route("/browse")
    def browse():
        return render_template("browse.html")

    @app.route("/review")
    def review():
        return render_template("review.html")

    @app.route("/lightroom")
    def lightroom_page():
        _auto_open_tab("lightroom")
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
        _auto_open_tab("workspace")
        return render_template("workspace.html")

    @app.route("/compare")
    def compare():
        return render_template("compare.html")

    @app.route("/settings")
    def settings():
        _auto_open_tab("settings")
        return render_template("settings.html")

    @app.route("/shortcuts")
    def shortcuts_page():
        _auto_open_tab("shortcuts")
        return render_template("shortcuts.html")

    @app.route("/keywords")
    def keywords_page():
        _auto_open_tab("keywords")
        return render_template("keywords.html")

    @app.route("/jobs")
    def jobs_page():
        return render_template("jobs.html")

    @app.route("/duplicates")
    def duplicates_page():
        _auto_open_tab("duplicates")
        return render_template("duplicates.html")

    @app.route("/move")
    def move_page():
        return render_template("move.html")

    @app.route("/highlights")
    def highlights_page():
        return render_template("highlights.html")

    @app.route("/misses")
    def misses_page():
        return render_template("misses.html")

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

    def _attach_detections(db, photo_dicts):
        """Attach detection bounding boxes to a list of photo dicts (in-place).

        Each photo gets a `detections` list of {x, y, w, h, confidence,
        category} dicts, ordered by confidence DESC. Photos with no
        detections get an empty list.
        """
        if not photo_dicts:
            return photo_dicts
        ids = [p["id"] for p in photo_dicts]
        det_map = db.get_detections_for_photos(ids)
        for p in photo_dicts:
            p["detections"] = det_map.get(p["id"], [])
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
        _attach_detections(db, photo_dicts)

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

        from taxonomy import find_taxonomy_json
        taxonomy_path = find_taxonomy_json()
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
            "recent_destinations": effective_cfg.get("ingest", {}).get("recent_destinations", []),
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

    @app.route("/api/folders/check-health", methods=["POST"])
    def api_folders_check_health():
        db = _get_db()
        changed = db.check_folder_health()
        missing = db.get_missing_folders()
        return jsonify({
            "changed": changed,
            "missing": [dict(f) for f in missing],
        })

    @app.route("/api/folders/<int:folder_id>", methods=["GET"])
    def api_folder_get(folder_id):
        """Return a single folder's id, name, and path.

        Powers the folder-tree context menu's "Copy Path" action. A lean
        response on purpose: callers that want the richer tree data already
        have /api/folders for that. Scoped to the active workspace so
        absolute paths from folders hidden in this workspace don't leak.
        """
        db = _get_db()
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

    @app.route("/api/folders/<int:folder_id>/relocate", methods=["POST"])
    def api_folder_relocate(folder_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        new_path = body.get("path", "")
        if not new_path:
            return json_error("path is required")
        if not os.path.isdir(new_path):
            return json_error("path does not exist or is not a directory")

        # Capture the old path before the DB rewrite so we can rebase the
        # corresponding darktable output subdir on disk. Developed outputs
        # are nested under developed_folder_key(folder_path), so a path
        # change invalidates the old key and would silently regress export
        # to RAW until the user re-developed.
        old_row = db.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        old_path = old_row["path"] if old_row else ""

        try:
            cascaded = db.relocate_folder(folder_id, new_path)
        except ValueError as e:
            return json_error(str(e), 409)

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

    @app.route("/api/folders/<int:folder_id>", methods=["DELETE"])
    def api_folder_delete(folder_id):
        db = _get_db()
        result = db.delete_folder(folder_id)
        # Clean up cached files alongside the cascaded photo rows so preview
        # files don't get orphaned on disk (untracked by preview_cache).
        _cleanup_cached_files_for_deleted_photos(result.get("files", []))
        # Don't leak the internal file list to the API response — keep the
        # shape callers expect.
        return jsonify({"deleted_photos": result["deleted_photos"]})

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
        _attach_detections(db, photo_dicts)

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

        # Location section: pre-resolved leaf + parent chain so the photo
        # detail panel can render the filled state without a second roundtrip.
        result["location"] = _serialize_photo_location(db, photo_id)

        # Read XMP sidecar keywords
        folder = db.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
        ).fetchone()
        if folder:
            # Full on-disk path: mirrors the folder-join logic in
            # api_files_reveal. Exposed so the browse-grid "Copy Path"
            # right-click action can read a real filesystem path from the
            # detail response.
            result["path"] = os.path.join(folder["path"], photo["filename"])
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
            result["path"] = ""
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

    def _queue_keyword_add(photo_id, keyword_name, workspace_id=None, _commit=True):
        """Queue a keyword add unless it cancels a pending removal."""
        db = _get_db()
        removed = db.remove_pending_changes(
            photo_id, "keyword_remove", keyword_name,
            workspace_id=workspace_id, _commit=_commit,
        )
        if removed == 0:
            db.queue_change(
                photo_id, "keyword_add", keyword_name,
                workspace_id=workspace_id, _commit=_commit,
            )

    def _queue_keyword_remove(photo_id, keyword_name, workspace_id=None, _commit=True):
        """Queue a keyword removal unless it cancels a pending add."""
        db = _get_db()
        removed = db.remove_pending_changes(
            photo_id, "keyword_add", keyword_name,
            workspace_id=workspace_id, _commit=_commit,
        )
        if removed == 0:
            db.queue_change(
                photo_id, "keyword_remove", keyword_name,
                workspace_id=workspace_id, _commit=_commit,
            )

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
        try:
            db.update_photo_rating(photo_id, rating)
        except ValueError as e:
            return json_error(str(e), 403)
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
        try:
            db.update_photo_flag(photo_id, flag)
        except ValueError as e:
            return json_error(str(e), 403)
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

    @app.route("/api/files/reveal", methods=["POST"])
    def api_files_reveal():
        """Reveal a photo or folder in the OS file manager.

        Body: {"photo_id": <int>} OR {"folder_id": <int>}

        Photo reveals select the file in its parent directory (macOS ``open
        -R``, Windows ``explorer /select,``, Linux ``xdg-open <parent dir>``).
        Folder reveals open the folder itself (macOS ``open -R <dir>``,
        Windows ``explorer <dir>``, Linux ``xdg-open <dir>``) — this differs
        from the photo case on Windows where we deliberately skip ``/select,``
        so the user sees the folder's contents rather than its parent.

        Returns: {"ok": True} on success; {"ok": False, "reason": "..."} if
        the subprocess failed to launch; 404 if the id is unknown; 400 if
        neither id was provided or either is malformed.
        """
        body = request.get_json(silent=True) or {}
        pid_raw = body.get("photo_id")
        fid_raw = body.get("folder_id")

        if pid_raw is None and fid_raw is None:
            return json_error("photo_id or folder_id required")

        db = _get_db()
        is_folder = False
        path = ""

        if pid_raw is not None:
            try:
                pid_int = int(pid_raw)
            except (TypeError, ValueError):
                return json_error("photo_id must be an integer")
            # verify_workspace=True enforces that the photo's folder is
            # linked to the active workspace — otherwise this endpoint would
            # expose absolute filesystem paths for photos hidden from the
            # current workspace.
            photo = db.get_photo(pid_int, verify_workspace=True)
            if not photo:
                return json_error("photo not found", 404)
            folder_row = db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
            ).fetchone()
            folder_path = folder_row["path"] if folder_row else ""
            if not folder_path or not photo["filename"]:
                return jsonify({"ok": False, "reason": "no path"})
            path = os.path.join(folder_path, photo["filename"])
        else:
            try:
                fid_int = int(fid_raw)
            except (TypeError, ValueError):
                return json_error("folder_id must be an integer")
            folder = db.get_folder(fid_int)
            if not folder:
                return json_error("folder not found", 404)
            # Reject reveal for folders not linked to the active workspace,
            # matching the photo branch's verify_workspace gate.
            linked = db.conn.execute(
                "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                (db._active_workspace_id, fid_int),
            ).fetchone()
            if not linked:
                return json_error("folder not found", 404)
            folder_path = folder["path"]
            if not folder_path:
                return jsonify({"ok": False, "reason": "no path"})
            path = folder_path
            is_folder = True

        try:
            if sys.platform == "darwin":
                # "open -R <dir>" reveals the folder inside its parent; that's
                # the right behavior for folder reveals too, consistent with
                # how Finder treats folder-targeted reveal.
                subprocess.run(["open", "-R", "--", path], timeout=5, check=False)
            elif sys.platform.startswith("win"):
                if is_folder:
                    # Open the folder itself so the user sees its contents.
                    subprocess.run(["explorer", path], timeout=5, check=False)
                else:
                    subprocess.run(
                        ["explorer", f"/select,{path}"], timeout=5, check=False
                    )
            else:
                # xdg-open on a file has inconsistent behavior across desktops
                # (some open the image viewer, not the file manager), so for
                # photo reveals we open the parent directory instead. Passing
                # a directory to xdg-open opens the folder in the file manager,
                # which is exactly what we want for folder reveals.
                target = path if is_folder else (os.path.dirname(path) or path)
                # xdg-open doesn't honor `--`; abspath guarantees a leading `/`
                # so a crafted leading-dash path can't be parsed as a flag.
                target = os.path.abspath(target)
                subprocess.run(["xdg-open", target], timeout=5, check=False)
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
            return jsonify({"ok": False, "reason": str(exc)})
        return jsonify({"ok": True})

    @app.route("/api/photos/<int:photo_id>/keywords", methods=["POST"])
    def api_add_keyword(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return json_error("name required")
        # Validate kw_type at the boundary (isinstance guard against
        # non-hashable JSON; membership against the canonical enum).
        # Pass it through to add_keyword so its type-reconciliation logic
        # runs — a post-update SQL `UPDATE keywords SET type = ?` would
        # silently rewrite an existing user-typed row (e.g. someone's
        # 'individual' Charlie) and bypass the taxonomy/general upgrade
        # rules in add_keyword.
        kw_type_raw = body.get("type")
        kw_type = (
            kw_type_raw
            if isinstance(kw_type_raw, str) and kw_type_raw in KEYWORD_TYPES
            else None
        )
        kid = db.add_keyword(name, kw_type=kw_type)
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

    def _summarize_details(details):
        """Build a short human-friendly summary string from a Place Details dict.

        Format: ``"<leaf name> · <broadest 1-2 parents>"``. Google's
        ``address_components`` are ordered narrowest-first, so the broadest
        parents (country, state) sit at the END of the list. We pick at most
        the last two, dedupe against the leaf name, and join with " · ".

        Examples::

            "Central Park · New York · United States"
            "Some Lighthouse · Iceland"
            "JustALeaf"  # if no usable parent components
        """
        leaf = (details or {}).get("name", "") or ""
        components = (details or {}).get("address_components") or []

        # Broadest 1-2 parents = last two components (Google orders broad-last).
        tail = components[-2:] if len(components) >= 2 else components[-1:]
        # Walk in reverse so we render broadest-first to broader-second
        # ("New York · United States" reads better than "United States · New York"
        # given the leaf comes first; iNaturalist uses leaf-then-narrowest-up).
        # Actually: leaf · narrowest-parent · ... · broadest-parent reads most
        # naturally for breadcrumbs. So reverse the tail so the closest parent
        # is first.
        parts = [leaf] if leaf else []
        for comp in reversed(tail):
            name = (comp or {}).get("name") or (comp or {}).get("long_name") or ""
            if not name:
                continue
            if name == leaf or name in parts:
                continue
            parts.append(name)

        if not parts:
            return ""
        return " · ".join(parts)

    def _walk_parent_chain(db, leaf_parent_id):
        """Walk ``parent_id`` upward from ``leaf_parent_id`` to the root.

        Returns a list of ``{"id": int, "name": str}`` dicts in broadest →
        narrowest order, EXCLUDING the leaf itself. Pass the leaf's
        ``parent_id`` (i.e. the *first* parent), not the leaf's own id.

        Depth cap of 10 — chains are bounded ~5 in practice, but guard
        against pathological/malformed cycles (link_keyword_to_place
        already prevents creating cycles, but a corrupted DB could).
        """
        parents = []
        current_parent_id = leaf_parent_id
        for _ in range(10):
            if current_parent_id is None:
                break
            row = db.conn.execute(
                "SELECT id, name, parent_id FROM keywords WHERE id = ?",
                (current_parent_id,),
            ).fetchone()
            if row is None:
                break
            parents.append({"id": row["id"], "name": row["name"]})
            current_parent_id = row["parent_id"]
        # Reverse so broadest (e.g. country) comes first, narrowest last.
        parents.reverse()
        return parents

    def _serialize_photo_location(db, photo_id):
        """Return a summary dict for the photo's current location keyword.

        The shape matches the JSON the location section UI expects::

            {
                "keyword_id":   int,
                "name":         str,
                "place_id":     str | None,
                "latitude":     float | None,
                "longitude":    float | None,
                "parent_chain": [{"id": int, "name": str}, ...],  # broadest -> narrowest, EXCLUDES leaf
            }

        Returns ``None`` if the photo has no ``type='location'`` keyword link.
        """
        leaf = db.conn.execute(
            "SELECT k.id, k.name, k.place_id, k.latitude, k.longitude, k.parent_id "
            "FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location' "
            "LIMIT 1",
            (photo_id,),
        ).fetchone()
        if leaf is None:
            return None

        return {
            "keyword_id": leaf["id"],
            "name": leaf["name"],
            "place_id": leaf["place_id"],
            "latitude": leaf["latitude"],
            "longitude": leaf["longitude"],
            "parent_chain": _walk_parent_chain(db, leaf["parent_id"]),
        }

    def _serialize_keyword(db, keyword_id):
        """Return a summary dict for a single ``type='location'`` keyword row.

        Same shape as :func:`_serialize_photo_location` (leaf fields + a
        broadest-first parent chain), but keyed on the keyword id directly
        rather than via a photo. Used by the ``link-place`` route, which
        operates on a keyword and isn't tied to a photo.

        Returns ``None`` if the keyword does not exist.
        """
        leaf = db.conn.execute(
            "SELECT id, name, place_id, latitude, longitude, parent_id "
            "FROM keywords WHERE id = ?",
            (keyword_id,),
        ).fetchone()
        if leaf is None:
            return None
        return {
            "keyword_id": leaf["id"],
            "name": leaf["name"],
            "place_id": leaf["place_id"],
            "latitude": leaf["latitude"],
            "longitude": leaf["longitude"],
            "parent_chain": _walk_parent_chain(db, leaf["parent_id"]),
        }

    # Location keywords don't propagate to dc:subject sidecars — structured XMP (exif:GPS*, Iptc4xmpCore:Location) is a future feature.
    @app.route("/api/photos/<int:photo_id>/location", methods=["POST"])
    def api_set_photo_location(photo_id):
        """Attach a Google place to ``photo_id`` via the autocomplete flow.

        Body: ``{"place_id": "ChIJ..."}``. Looks up the place via
        :func:`places.place_details`, upserts the leaf + parent chain into
        ``keywords``, and links the leaf to the photo (replacing any existing
        ``type='location'`` link).
        """
        body = request.get_json(silent=True) or {}
        place_id = (body.get("place_id") or "").strip()
        if not place_id:
            return json_error("missing place_id", 400)

        import config as cfg
        key = cfg.load().get("google_maps_api_key", "")
        if not key:
            return json_error("no_api_key", 400)

        db = _get_db()
        # Guard against stale clients (e.g. tab open after photo deleted).
        # Without this, set_photo_location's INSERT into photo_keywords
        # raises a FK IntegrityError that surfaces as a 500.
        if db.conn.execute(
            "SELECT 1 FROM photos WHERE id = ?", (photo_id,)
        ).fetchone() is None:
            return json_error("photo_not_found", 404)

        details = places.place_details(place_id, key)
        if details is None:
            return json_error("place_not_found", 404)

        try:
            leaf_id = db.upsert_place_chain(details)
        except RuntimeError as err:
            # _upsert_one_keyword raises RuntimeError when the parent-chain
            # build hits an existing keyword of a different type at the same
            # (name, parent_id). Mirror /api/keywords/<id>/link-place's 409.
            return jsonify({
                "error": "name_conflict",
                "error_detail": str(err),
            }), 409
        db.set_photo_location(photo_id, leaf_id)
        db.record_edit(
            'location_set',
            f"set location: {details.get('name', 'unknown')}",
            str(leaf_id),
            [{'photo_id': photo_id, 'old_value': '', 'new_value': str(leaf_id)}],
        )
        return jsonify({"location": _serialize_photo_location(db, photo_id)})

    @app.route("/api/photos/<int:photo_id>/location/text", methods=["POST"])
    def api_set_photo_location_text(photo_id):
        """Attach a free-text location keyword (no Google data) to ``photo_id``.

        Body: ``{"name": "the meadow behind the cabin"}``. Used when the user
        types a name and hits Enter without picking a Google suggestion, or
        when no API key is configured. Reuses the ``location_set`` audit
        action_type so the audit log filters consistently across both paths.
        """
        body = request.get_json(silent=True) or {}
        name = body.get("name") or ""
        if not name.strip():
            return json_error("missing name", 400)
        stripped = name.strip()

        db = _get_db()
        if db.conn.execute(
            "SELECT 1 FROM photos WHERE id = ?", (photo_id,)
        ).fetchone() is None:
            return json_error("photo_not_found", 404)
        try:
            leaf_id = db.get_or_create_text_location(stripped)
        except ValueError:
            # Defensive: validation above should already catch empty input.
            return json_error("missing name", 400)
        db.set_photo_location(photo_id, leaf_id)
        db.record_edit(
            'location_set',
            f"set location: {stripped}",
            str(leaf_id),
            [{'photo_id': photo_id, 'old_value': '', 'new_value': str(leaf_id)}],
        )
        return jsonify({"location": _serialize_photo_location(db, photo_id)})

    @app.route("/api/photos/<int:photo_id>/location", methods=["DELETE"])
    def api_clear_photo_location(photo_id):
        """Remove all ``type='location'`` keyword links for ``photo_id``."""
        db = _get_db()
        if db.conn.execute(
            "SELECT 1 FROM photos WHERE id = ?", (photo_id,)
        ).fetchone() is None:
            return json_error("photo_not_found", 404)
        db.clear_photo_location(photo_id)
        db.record_edit(
            'location_clear',
            "cleared location",
            '',
            [{'photo_id': photo_id, 'old_value': '', 'new_value': ''}],
        )
        return jsonify({"ok": True})

    @app.route("/api/places/reverse-geocode", methods=["GET"])
    def api_reverse_geocode():
        """Reverse-geocode (lat, lng) via Google, with a SQLite grid cache.

        Query params: ``lat``, ``lng`` (floats). Returns
        ``{"place_id": <str|null>, "summary": <str|null>}``.

        Cache layer is keyed on ~110m grid (see ``Database._reverse_geocode_grid``).
        A row with ``place_id=None`` is a cached negative — Google was previously
        asked and had no match, and we serve null without re-asking.

        When no ``google_maps_api_key`` is configured we degrade to ``null``
        WITHOUT writing to the cache. Caching null in that branch would make
        already-asked grids stay null forever once the user finally adds a
        key, which is exactly the wrong UX.
        """
        try:
            lat = float(request.args.get("lat", ""))
            lng = float(request.args.get("lng", ""))
        except (TypeError, ValueError):
            return json_error("invalid coords", 400)
        # float() accepts "nan" and "inf"; both blow up downstream in
        # _reverse_geocode_grid's int(round(...)). Reject explicitly.
        if not (math.isfinite(lat) and math.isfinite(lng)):
            return json_error("invalid coords", 400)

        db = _get_db()
        cached = db.reverse_geocode_cache_get(lat, lng)
        if cached is not None:
            if cached["place_id"] is None:
                # Cached negative — Google previously had no match here.
                return jsonify({"place_id": None, "summary": None})
            try:
                details = json.loads(cached["response"])
            except (ValueError, TypeError):
                details = {}
            return jsonify({
                "place_id": cached["place_id"],
                "summary": _summarize_details(details),
            })

        # Cache miss.
        import config as cfg
        key = cfg.load().get("google_maps_api_key", "")
        if not key:
            # Don't cache here — see docstring.
            return jsonify({"place_id": None, "summary": None})

        try:
            details = places.reverse_geocode(lat, lng, key)
        except places.PlacesTransientError:
            # OVER_QUERY_LIMIT / REQUEST_DENIED / network blip — Google
            # may answer this later, so do NOT cache. Returning null here
            # just suppresses the EXIF suggestion for this request; the
            # next request will retry Google.
            app.logger.warning(
                "reverse_geocode transient failure for lat=%s lng=%s — not caching",
                lat, lng,
            )
            return jsonify({"place_id": None, "summary": None})

        cache_place_id = details.get("place_id") if details else None
        db.reverse_geocode_cache_put(
            lat, lng,
            place_id=cache_place_id,
            response_json=json.dumps(details or {}),
        )
        if details is None:
            return jsonify({"place_id": None, "summary": None})
        return jsonify({
            "place_id": cache_place_id,
            "summary": _summarize_details(details),
        })

    @app.route("/api/keywords/<int:keyword_id>/link-place", methods=["POST"])
    def api_link_keyword_to_place(keyword_id):
        """Attach Google place data to an existing keyword.

        Body: ``{"place_id": "ChIJ..."}``. Looks up the place via
        :func:`places.place_details` and delegates to
        :meth:`Database.link_keyword_to_place`, which UPDATEs the target row
        in-place — or, if another keyword already owns this ``place_id``,
        re-points the target's ``photo_keywords`` rows onto the canonical row
        and deletes the now-empty target.

        Response: ``{"keyword": <serialized leaf+chain>, "merged": <bool>}``.
        ``merged`` is True when an existing place-bearing row absorbed the
        target.

        Error modes:
        - 400 ``missing place_id`` — empty body.
        - 400 ``no_api_key`` — config has no ``google_maps_api_key``.
        - 404 ``place_not_found`` — Google had no record of ``place_id``.
        - 404 ``keyword_not_found`` — ``keyword_id`` doesn't exist.
        - 409 ``name_conflict`` — the parent chain would clash with an
          existing keyword of a different ``type`` at the same
          ``(name, parent_id)``. Carries an ``error_detail`` string from the
          underlying RuntimeError for debugging.
        """
        body = request.get_json(silent=True) or {}
        place_id = (body.get("place_id") or "").strip()
        if not place_id:
            return json_error("missing place_id", 400)

        import config as cfg
        key = cfg.load().get("google_maps_api_key", "")
        if not key:
            return json_error("no_api_key", 400)

        details = places.place_details(place_id, key)
        if details is None:
            return json_error("place_not_found", 404)

        db = _get_db()
        try:
            result = db.link_keyword_to_place(keyword_id, details)
        except ValueError as err:
            # Database raises ValueError both for "missing id" and "wrong
            # type". Distinguish so callers can tell a 404 from a 400.
            msg = str(err)
            if "is type" in msg:
                return jsonify({
                    "error": "wrong_keyword_type",
                    "error_detail": msg,
                }), 400
            return json_error("keyword_not_found", 404)
        except RuntimeError as err:
            # _upsert_one_keyword raises RuntimeError when the parent-chain
            # build hits an existing keyword of a different type at the same
            # (name, parent_id). Surface the message for debugging.
            return jsonify({
                "error": "name_conflict",
                "error_detail": str(err),
            }), 409

        # Audit log: this action isn't tied to a single photo (it operates on
        # a keyword), so we omit the per-photo items list. ``photo_id`` in
        # ``edit_history_items`` is FK-constrained to ``photos.id``, so a
        # placeholder like 0 would IntegrityError. The ``new_value`` column on
        # the parent ``edit_history`` row carries the canonical keyword id;
        # the ``description`` carries the place name and the source-id pair.
        db.record_edit(
            'location_link',
            (
                f"linked keyword {keyword_id} to place: "
                f"{details.get('name', 'unknown')} "
                f"(canonical keyword_id={result['keyword_id']}, "
                f"merged={result['merged']})"
            ),
            str(result['keyword_id']),
            [],
        )

        return jsonify({
            "keyword": _serialize_keyword(db, result["keyword_id"]),
            "merged": result["merged"],
        })

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
        try:
            db.batch_update_photo_rating(valid_ids, rating)
        except ValueError as e:
            return json_error(str(e), 403)
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
        try:
            db.batch_update_photo_flag(valid_ids, flag)
        except ValueError as e:
            return json_error(str(e), 403)
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
        # Route kw_type through add_keyword so its type-reconciliation logic
        # runs (preserves existing user-typed rows; only upgrades 'general').
        # See api_add_keyword for the rationale.
        kw_type_raw = body.get("type")
        kw_type = (
            kw_type_raw
            if isinstance(kw_type_raw, str) and kw_type_raw in KEYWORD_TYPES
            else None
        )
        kid = db.add_keyword(name, kw_type=kw_type)
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

        _cleanup_cached_files_for_deleted_photos(result["files"])

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
        limit = min(max(1, request.args.get("limit", 50, type=int)), 1000)
        offset = max(0, request.args.get("offset", 0, type=int))
        return jsonify(db.get_edit_history(limit=limit, offset=offset))

    # -- Statistics --

    @app.route("/api/stats")
    def api_stats():
        db = _get_db()
        stats = db.get_dashboard_stats()
        stats["total_photos"] = db.count_photos()
        return jsonify(stats)

    @app.route("/api/coverage")
    def api_coverage():
        """Return per-stage processing coverage for the active workspace.

        ``overall`` is the workspace-wide count for each pipeline stage, and
        ``folders`` is a per-folder breakdown (one row per top-level folder
        linked to the workspace). Both share the same coverage keys.
        """
        db = _get_db()
        return jsonify({
            "overall": db.get_coverage_stats(),
            "folders": db.get_folder_coverage_stats(),
        })

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

    @app.route("/api/collections/<int:collection_id>", methods=["PUT"])
    def api_update_collection(collection_id):
        """Rename a collection. Body: {"name": "..."}."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        if not name:
            return json_error("name required")
        try:
            db.rename_collection(collection_id, name)
        except ValueError:
            return json_error("collection not found", 404)
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
        # Refuse to mutate smart/system collections like "All Photos" — adding
        # a photo_ids rule would AND-combine with the sentinel and silently
        # convert the dynamic default into a static subset.
        if any(r.get("field") == "all" for r in rules):
            return json_error("Cannot add photos to this collection", 400)
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

    @app.route("/api/collections/<int:collection_id>/duplicate", methods=["POST"])
    def api_collection_duplicate(collection_id):
        """Duplicate a collection within the active workspace. Returns {id}."""
        db = _get_db()
        try:
            new_id = db.duplicate_collection(collection_id)
        except ValueError:
            return json_error("collection not found", 404)
        return jsonify({"ok": True, "id": new_id})

    @app.route("/api/collections/<int:collection_id>/photos")
    def api_collection_photos(collection_id):
        import config as cfg
        db = _get_db()
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), _MAX_PER_PAGE))
        photos = db.get_collection_photos(collection_id, page=page, per_page=per_page)
        total = db.count_collection_photos(collection_id)
        photo_dicts = [dict(p) for p in photos]
        _attach_species(db, photo_dicts)
        _attach_detections(db, photo_dicts)
        return jsonify(
            {
                "photos": photo_dicts,
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
                "scope": "folder",
            })

        # scope=workspace blends candidates from every folder in the active
        # workspace. This matches how photoshoots land in Vireo: a single
        # shoot often spans multiple dated folders (YYYY-MM-DD subfolders),
        # so one folder != one shoot.
        scope = request.args.get("scope", "folder")
        folder_id = request.args.get("folder_id", type=int)
        if scope == "workspace":
            folder_id = None
        elif folder_id is None:
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
            "scope": "workspace" if folder_id is None else "folder",
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
        allowed = {"classification_threshold", "grouping_window_seconds", "similarity_threshold", "detector_confidence", "review_min_confidence"}
        # Share the schema-driven settings write lock so an autosave in the
        # All-settings region can't race with a curated workspace-form save
        # and silently drop a recent override.
        with _settings_write_lock:
            ws = db.get_workspace(db._active_workspace_id)
            existing = {}
            if ws and ws["config_overrides"]:
                try:
                    existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except Exception:
                    pass
            if not isinstance(existing, dict):
                existing = {}
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
            return json_error("nav_order must be a list")
        # Share the schema-driven settings write lock so a concurrent schema
        # autosave can't read this same overrides snapshot and overwrite the
        # nav-order change with stale data.
        with _settings_write_lock:
            ws = db.get_workspace(db._active_workspace_id)
            existing = {}
            if ws and ws["config_overrides"]:
                try:
                    existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except Exception:
                    pass
            if not isinstance(existing, dict):
                existing = {}
            existing["nav_order"] = nav_order
            db.update_workspace(db._active_workspace_id, config_overrides=existing)
        return jsonify({"ok": True, "nav_order": nav_order})

    @app.route("/api/workspaces/active/subject-types", methods=["GET"])
    def api_get_active_subject_types():
        """Return the active workspace's effective subject_types — global
        defaults merged with workspace overrides. The workspace settings UI
        needs this rather than just the override JSON, so checkboxes render
        the actual current state when the user has only customized at the
        global config layer."""
        db = _get_db()
        types = sorted(db.get_subject_types())
        return jsonify({"types": types})

    @app.route("/api/workspaces/<int:ws_id>/subject-types", methods=["PUT"])
    def api_set_subject_types(ws_id):
        """Set the subject_types config override for a workspace.

        Unknown type values are dropped (logged). An empty list is allowed
        (logged warning) and effectively disables the 'identified' filter.
        """
        from db import KEYWORD_TYPES
        db = _get_db()
        body = request.get_json(silent=True) or {}
        raw_types = body.get("types")
        if not isinstance(raw_types, list):
            return json_error("types must be a list")
        # Guard the membership test against non-string entries (e.g. nested
        # lists or objects). `x in frozenset` raises TypeError on unhashable
        # input — that would 500 the request instead of dropping the bad
        # element per the documented "unknown values are dropped" contract.
        cleaned = [t for t in raw_types if isinstance(t, str) and t in KEYWORD_TYPES]
        dropped = [t for t in raw_types if not isinstance(t, str) or t not in KEYWORD_TYPES]
        if dropped:
            log.warning(
                "subject-types: dropped unknown values %s for ws=%s",
                dropped, ws_id,
            )
        if not cleaned:
            log.warning("subject-types: empty list set for workspace %s", ws_id)
        # Share the schema-driven settings write lock so a concurrent
        # autosave on the same workspace can't read this same overrides
        # snapshot and overwrite our subject_types change with stale data.
        with _settings_write_lock:
            ws = db.get_workspace(ws_id)
            if not ws:
                return json_error("workspace not found", 404)
            existing = {}
            if ws["config_overrides"]:
                try:
                    existing = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except Exception:
                    existing = {}
            # `config_overrides` is JSON, so a previous PUT /api/workspaces/<id>
            # could have stored a list/string/number. Coerce to {} before key
            # assignment to keep this endpoint from 500-ing on malformed state.
            if not isinstance(existing, dict):
                existing = {}
            existing["subject_types"] = cleaned
            db.update_workspace(ws_id, config_overrides=existing)
        return jsonify({"types": cleaned})

    @app.route("/api/workspace/tabs/open", methods=["POST"])
    def api_open_tab():
        from db import OPENABLE_NAV_IDS
        db = _get_db()
        body = request.get_json(silent=True) or {}
        nav_id = body.get("nav_id")
        if nav_id not in OPENABLE_NAV_IDS:
            return json_error("nav_id is not openable", 400)
        tabs = db.open_tab(nav_id)
        return jsonify({"ok": True, "open_tabs": tabs})

    @app.route("/api/workspace/tabs/close", methods=["POST"])
    def api_close_tab():
        from db import OPENABLE_NAV_IDS
        db = _get_db()
        body = request.get_json(silent=True) or {}
        nav_id = body.get("nav_id")
        if nav_id not in OPENABLE_NAV_IDS:
            return json_error("nav_id is not openable", 400)
        tabs = db.close_tab(nav_id)
        return jsonify({"ok": True, "open_tabs": tabs})

    @app.route("/api/workspace/tabs", methods=["GET"])
    def api_get_tabs():
        db = _get_db()
        try:
            open_tabs = db.get_open_tabs()
        except Exception:
            open_tabs = []
        TOOLS_ORDER = ["settings", "workspace", "lightroom",
                       "shortcuts", "keywords", "duplicates", "logs"]
        TAB_LABELS = {
            "settings": "Settings", "workspace": "Workspace",
            "lightroom": "Lightroom", "shortcuts": "Shortcuts",
            "keywords": "Keywords", "duplicates": "Duplicates", "logs": "Logs",
        }
        openable_pages = [
            {"id": t, "label": TAB_LABELS[t], "href": "/" + t}
            for t in TOOLS_ORDER
        ]
        return jsonify({"open_tabs": open_tabs, "openable_pages": openable_pages})

    @app.route("/api/workspaces/active/new-images")
    def api_workspace_new_images():
        db = _get_db()
        ws_id = db._active_workspace_id
        if ws_id is None:
            return jsonify({"workspace_id": None, "new_count": 0, "per_root": [], "sample": []})

        cache = db._new_images_cache
        db_path = db._db_path
        cached = cache.get(db_path, ws_id)
        if cached is not None:
            payload = dict(cached)
            payload["workspace_id"] = ws_id
            return jsonify(payload)

        # If a recent compute failed and we're still inside the backoff
        # window, surface the error instead of kicking off another walk.
        # Without this, the navbar's pending re-poll keeps hammering a
        # broken volume / DB and the UI is stuck "checking" forever.
        recent_err = cache.get_recent_error(db_path, ws_id)
        if recent_err is not None:
            return jsonify({
                "workspace_id": ws_id,
                "new_count": None,
                "per_root": [],
                "sample": [],
                "error": recent_err,
            })

        # Cache cold: run the filesystem walk in a background thread so the
        # navbar's poll doesn't tie up a Flask worker for seconds while
        # os.walk grinds through a large library. Wait briefly so small
        # libraries (and the test suite's tmp_path filesystems) still
        # observe a real count synchronously; longer walks return
        # ``pending: true`` and the front-end re-polls.
        from db import Database
        from new_images import count_new_images_for_workspace

        # Shared holder for the cache worker's exception (if any), read by
        # the ephemeral job's work_fn after the cache event fires. Without
        # this, a walk that raises (e.g. unreadable volume, DB error) would
        # still mark the job ``completed`` while ``/api/.../new-images``
        # returns the error from ``get_recent_error`` — contradictory state
        # in the bottom panel.
        walk_error = {"exc": None}

        def compute(progress_callback=None):
            wdb = Database(db_path)
            wdb.set_active_workspace(ws_id)
            try:
                return count_new_images_for_workspace(
                    wdb, ws_id, progress_callback=progress_callback,
                )
            except Exception as e:
                walk_error["exc"] = e
                raise

        # Surface the walk as an ephemeral job so the user can see it in the
        # bottom panel rather than wondering why their workspace is silent.
        # ``on_spawn`` only fires when this kickoff actually starts a new
        # worker (cache truly cold) — cache hits and reuse of an in-flight
        # walk skip job creation, so navbar polls don't clutter the list.
        ws_row = db.get_workspace(ws_id)
        ws_name = ws_row["name"] if ws_row else f"workspace #{ws_id}"
        runner = app._job_runner

        def on_spawn(spawn_event):
            progress_state = {"checked": 0, "found": 0}

            def job_work_fn(job):
                # Mirror the cache worker's lifecycle. ``spawn_event`` fires
                # in the worker's finally clause, so we wake when the walk
                # ends regardless of success or failure. Final totals come
                # from progress_state, which the cache worker populated via
                # progress_callback. If the walk raised, re-raise the same
                # exception so JobRunner marks the job ``failed`` with the
                # original message — keeping the bottom panel and the
                # ``/api/.../new-images`` payload in agreement.
                spawn_event.wait()
                if walk_error["exc"] is not None:
                    raise walk_error["exc"]
                return {
                    "files_checked": progress_state["checked"],
                    "new_count": progress_state["found"],
                }

            job_id = runner.start(
                "new_images_walk",
                job_work_fn,
                ephemeral=True,
                workspace_id=ws_id,
                config={"workspace_name": ws_name},
            )

            def progress_callback(files_checked, new_found):
                progress_state["checked"] = files_checked
                progress_state["found"] = new_found
                runner.push_event(
                    job_id,
                    "progress",
                    {
                        "current": files_checked,
                        "total": 0,
                        "phase": (
                            f"{files_checked:,} checked, {new_found:,} new"
                        ),
                        "files_checked": files_checked,
                        "new_count": new_found,
                    },
                )

            return progress_callback

        event = cache.kickoff_compute(db_path, ws_id, compute, on_spawn=on_spawn)
        if event.wait(timeout=0.5):
            cached = cache.get(db_path, ws_id)
            if cached is not None:
                payload = dict(cached)
                payload["workspace_id"] = ws_id
                return jsonify(payload)
            # Compute finished but produced no cached entry — it must have
            # raised. Surface the error captured by the worker.
            recent_err = cache.get_recent_error(db_path, ws_id)
            if recent_err is not None:
                return jsonify({
                    "workspace_id": ws_id,
                    "new_count": None,
                    "per_root": [],
                    "sample": [],
                    "error": recent_err,
                })

        return jsonify({
            "workspace_id": ws_id,
            "new_count": None,
            "per_root": [],
            "sample": [],
            "pending": True,
        })

    @app.route("/api/workspaces/active/new-images/snapshot", methods=["POST"])
    def api_workspace_new_images_snapshot_create():
        db = _get_db()
        ws_id = db._active_workspace_id
        if ws_id is None:
            return jsonify({"error": "no active workspace"}), 400
        from new_images import count_new_images_for_workspace
        result = count_new_images_for_workspace(db, ws_id, sample_limit=None)
        file_paths = list(result["sample"])
        snap_id = db.create_new_images_snapshot(file_paths)
        folders = sorted({os.path.dirname(p) for p in file_paths})
        return jsonify({
            "snapshot_id": snap_id,
            "file_count": len(file_paths),
            "folders": folders,
        })

    @app.route(
        "/api/workspaces/active/new-images/snapshot/<int:snapshot_id>",
        methods=["GET"],
    )
    def api_workspace_new_images_snapshot_get(snapshot_id):
        db = _get_db()
        if db._active_workspace_id is None:
            abort(404)
        snap = db.get_new_images_snapshot(snapshot_id)
        if snap is None:
            abort(404)
        paths = snap["file_paths"]
        folder_paths = sorted({os.path.dirname(p) for p in paths})
        files_sample = paths[:5]
        return jsonify({
            "file_count": snap["file_count"],
            "folder_paths": folder_paths,
            "files_sample": files_sample,
        })

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
            return json_error("collection_id required")

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
        # Review state lives in prediction_review now; predictions.model is
        # renamed to classifier_model.  Sibling-alternative rejection goes
        # through the workspace-scoped review table.
        ws = db._ws_id()
        pred = db.conn.execute(
            """SELECT pr.id, pr.species, pr.detection_id,
                      pr.classifier_model AS model,
                      pr.labels_fingerprint, d.photo_id
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               WHERE pr.id = ?""",
            (pred_id,),
        ).fetchone()
        # prediction_review has an FK on prediction_id; writing review state
        # for a missing pred would raise an IntegrityError and return 500
        # where the legacy endpoint returned a harmless no-op. Gate the
        # write on existence so stale IDs stay a clean 404.
        if pred is None:
            return json_error("prediction not found", 404)
        db.update_prediction_status(pred_id, "rejected", _commit=False)
        # Also reject sibling alternative predictions for the same
        # (detection, classifier_model, labels_fingerprint) in this
        # workspace. Fingerprint scoping matters: without it, rejecting a
        # prediction from a new label set would rewrite review state for
        # prior fingerprints' alternatives on the same detection.
        sibling_ids = [row["id"] for row in db.conn.execute(
            """SELECT pr.id
               FROM predictions pr
               JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id
                AND pr_rev.workspace_id = ?
               WHERE pr.detection_id = ?
                 AND pr.classifier_model = ?
                 AND pr.labels_fingerprint = ?
                 AND pr.id != ?
                 AND pr_rev.status = 'alternative'""",
            (ws, pred["detection_id"], pred["model"],
             pred["labels_fingerprint"], pred_id),
        ).fetchall()]
        for sid in sibling_ids:
            db.update_prediction_status(sid, "rejected", _commit=False)
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

        # Pre-validate all photo IDs against workspace before any mutations
        for pid in picks + rejects:
            if not db._photo_in_workspace(pid):
                return json_error(f"Photo {pid} is not in the active workspace", 403)

        # Capture old flag values before mutation
        all_flag_pids = picks + rejects
        old_flags = {}
        for pid in all_flag_pids:
            old = db.get_photo(pid)
            if old:
                old_flags[pid] = old["flag"] or "none"

        # Flag picks and add species keyword
        try:
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
        except ValueError as e:
            return json_error(str(e), 403)

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

    @app.route("/api/misses")
    def api_misses():
        """Return photos flagged as misses.

        With no query string, returns a dict with all three categories.
        With ``?category=X``, returns {"photos": [...], "category": X}.
        ``?since=<iso-ts>`` restricts results to photos whose
        miss_computed_at >= since (used by the pipeline-review step).
        """
        db = _get_db()
        category = request.args.get("category")
        since = request.args.get("since") or None
        if category is not None:
            if category not in ("no_subject", "clipped", "oof"):
                return jsonify({"error": "invalid category"}), 400
            photos = db.list_misses(category=category, since=since)
            return jsonify({"photos": photos, "category": category})
        return jsonify({
            "no_subject": db.list_misses(category="no_subject", since=since),
            "clipped":    db.list_misses(category="clipped", since=since),
            "oof":        db.list_misses(category="oof", since=since),
        })

    @app.route("/api/misses/reject", methods=["POST"])
    def api_misses_reject():
        """Set flag='rejected' on every photo currently flagged with the given
        miss category.

        Accepts an optional ``since`` ISO timestamp that mirrors the
        ``/misses?since=...`` review-window scope; when present, only
        photos whose miss_computed_at >= since are rejected, so the bulk
        action matches what the user sees on screen. Returns
        {"rejected": n, "category": ...}.

        Records a batch ``flag`` entry in ``edit_history`` so the bulk
        change is undoable and shows up in the audit log, matching the
        behavior of ``/api/batch/flag``.
        """
        db = _get_db()
        body = request.get_json(silent=True) or {}
        category = body.get("category")
        since = body.get("since") or None
        if category not in ("no_subject", "clipped", "oof"):
            return jsonify({"error": "invalid category"}), 400
        affected = db.bulk_reject_miss_category(category, since=since)
        if affected:
            items = [
                {"photo_id": a["photo_id"],
                 "old_value": a["old_value"],
                 "new_value": "rejected"}
                for a in affected
            ]
            db.record_edit(
                "flag",
                f"Rejected {len(items)} miss photos (category={category})",
                "rejected",
                items,
                is_batch=True,
            )
        return jsonify({"rejected": len(affected), "category": category})

    @app.route("/api/misses/<int:photo_id>/unflag", methods=["POST"])
    def api_misses_unflag(photo_id):
        """Clear the given miss-category boolean on a single photo."""
        db = _get_db()
        body = request.get_json(silent=True) or {}
        category = body.get("category")
        if category not in ("no_subject", "clipped", "oof"):
            return jsonify({"error": "invalid category"}), 400
        try:
            db.clear_miss_flag(photo_id, category)
        except ValueError:
            return jsonify({"error": "photo not in active workspace"}), 404
        return jsonify({"ok": True})

    @app.route("/api/classify/readiness")
    def api_classify_readiness():
        """Check what's ready for classification and what will need work."""
        from classifier import _embedding_cache_path, _resolve_model_dir
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
            model_dir = _resolve_model_dir(
                model.get("model_str", ""), model.get("weights_path")
            )
            cache_path = _embedding_cache_path(
                labels, model.get("model_str", ""), model_dir
            )
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

    @app.route("/api/pipeline/extract-readiness")
    def api_extract_readiness():
        """Report SAM2/DINOv2 download status for the Extract Features card."""
        from dino_embed import DINOV2_VARIANTS, dinov2_status
        from masking import SAM2_VARIANTS, sam2_status

        db = _get_db()
        pipeline_cfg = db.get_effective_config(cfg.load()).get("pipeline", {})
        sam2_variant = request.args.get("sam2_variant") or pipeline_cfg.get(
            "sam2_variant", "sam2-small"
        )
        dinov2_variant = request.args.get("dinov2_variant") or pipeline_cfg.get(
            "dinov2_variant", "vit-b14"
        )

        return jsonify({
            "sam2": sam2_status(sam2_variant),
            "sam2_known": sam2_variant in SAM2_VARIANTS,
            "dinov2": dinov2_status(dinov2_variant),
            "dinov2_known": dinov2_variant in DINOV2_VARIANTS,
        })

    # --- Eye-focus keypoint model download (opt-in) ---
    _ALLOWED_KEYPOINT_MODELS = (
        "superanimal-quadruped",
        "superanimal-bird",
        "rtmpose-animal",
    )

    @app.route("/api/models/keypoints/status")
    def api_keypoints_status():
        """Return download state for each eye-focus keypoint model."""
        import keypoints as kp

        return jsonify({
            name.replace("-", "_"): kp.weights_status(name)
            for name in _ALLOWED_KEYPOINT_MODELS
        })

    @app.route(
        "/api/models/keypoints/<model_name>/download",
        methods=["POST"],
    )
    def api_keypoints_download(model_name):
        """Trigger a background download of the named keypoint model.

        Rejects unknown model names so a bad argument can't be coerced into
        an arbitrary HuggingFace fetch. Returns immediately; the client
        polls /api/models/keypoints/status to observe progress.
        """
        import threading

        import keypoints as kp

        if model_name not in _ALLOWED_KEYPOINT_MODELS:
            return json_error(f"unknown keypoint model {model_name!r}", 400)

        current = kp.weights_status(model_name)
        if current in ("ready", "downloading"):
            return jsonify({"status": current})

        def _download():
            kp._download_state[model_name] = "downloading"
            try:
                kp.ensure_keypoint_weights(model_name)
                kp._download_state[model_name] = "idle"
            except Exception as exc:
                log.warning(
                    "Keypoint weights download failed for %s: %s",
                    model_name, exc,
                )
                kp._download_state[model_name] = "failed"

        threading.Thread(target=_download, daemon=True).start()
        return jsonify({"status": "downloading"})

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
        # Share the schema-driven settings write lock so an autosave in the
        # All-settings region can't race with the curated form's full-snapshot
        # save and silently overwrite a recently-saved schema value.
        with _settings_write_lock:
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
            # If the user shrunk the preview cache quota, evict immediately to the
            # new size rather than waiting for the next cache write. No-op when
            # already under quota, so always safe to call.
            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
            evict_preview_cache_if_over_quota(_get_db(), vireo_dir)
        return jsonify({"ok": True})

    @app.route("/api/settings/schema")
    def api_settings_schema():
        """Return the SCHEMA dict and the ordered category list.

        Consumed by the schema-rendered settings UI to generate widgets.
        """
        import config_schema as schema

        return jsonify({
            "schema": schema.SCHEMA,
            "categories": list(schema.CATEGORIES),
        })

    @app.route("/api/settings/values")
    def api_settings_values():
        """Return values across all four layers (default / global / workspace / effective).

        Each layer is a dotted-flat dict, restricted to keys present in SCHEMA.
        Keys hand-edited into config.json or stored as workspace metadata
        (e.g. active_labels) that are not declared in SCHEMA are intentionally
        omitted from the response so they don't show up as "overridden" in
        the UI; they are preserved on disk and visible in the raw-JSON tab.
        """
        import config as cfg
        import config_schema as schema

        schema_keys = set(schema.SCHEMA.keys())

        # Default layer: flatten DEFAULTS, restrict to schema keys.
        default_flat = schema.flatten(cfg.DEFAULTS)
        default_layer = {k: default_flat[k] for k in schema_keys if k in default_flat}

        # Global layer: read the raw file (not cfg.load(), which deep-merges
        # with DEFAULTS — we want only what the user explicitly set). Also
        # filter out keys whose value equals the default: legacy save paths
        # write the entire deep-merged config, but a value matching the
        # default is not really a user override and should not show as one.
        global_flat = schema.flatten(_read_raw_config_file())
        global_layer = {
            k: v for k, v in global_flat.items()
            if k in schema_keys and default_layer.get(k) != v
        }

        # Workspace layer: parse config_overrides for the active workspace.
        workspace_layer = {}
        db = _get_db()
        ws = db.get_workspace(db._active_workspace_id)
        if ws and ws["config_overrides"]:
            try:
                overrides = (
                    json.loads(ws["config_overrides"])
                    if isinstance(ws["config_overrides"], str)
                    else ws["config_overrides"]
                )
                ws_flat = schema.flatten(overrides if isinstance(overrides, dict) else {})
                # Filter out global-only schema keys: workspace create/update
                # APIs can persist arbitrary override payloads, so a workspace
                # may contain entries for keys whose scope is "global".
                # Runtime paths for those keys read global config only, so
                # surfacing the workspace value here would mislead the UI
                # into showing a workspace-effective value that is never
                # actually applied.
                workspace_layer = {
                    k: v for k, v in ws_flat.items()
                    if k in schema_keys
                    and schema.SCHEMA[k].get("scope") != "global"
                }
            except (json.JSONDecodeError, TypeError):
                workspace_layer = {}

        # Effective layer: workspace > global > default for every schema key.
        effective_layer = {}
        for k in schema_keys:
            if k in workspace_layer:
                effective_layer[k] = workspace_layer[k]
            elif k in global_layer:
                effective_layer[k] = global_layer[k]
            elif k in default_layer:
                effective_layer[k] = default_layer[k]

        return jsonify({
            "default": default_layer,
            "global": global_layer,
            "workspace": workspace_layer,
            "effective": effective_layer,
        })

    def _settings_post_save_side_effects(current):
        """Side effects mirrored from the legacy /api/config POST handler.

        Keeps the new schema-driven write path behaviorally identical to the
        old curated-form save: HF_TOKEN env var is kept in sync, and the
        preview cache is evicted if its budget shrunk.
        """
        hf_token = current.get("hf_token", "")
        if hf_token:
            os.environ["HF_TOKEN"] = hf_token
        elif "HF_TOKEN" in os.environ:
            del os.environ["HF_TOKEN"]
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        evict_preview_cache_if_over_quota(_get_db(), vireo_dir)

    def _read_raw_config_file():
        """Return the parsed contents of ~/.vireo/config.json, or {}.

        Unlike cfg.load(), this does NOT merge DEFAULTS — so it contains
        only the keys the user has actually set. Used by write paths so the
        on-disk file stays minimal.
        """
        import config as cfg

        if not os.path.exists(cfg.CONFIG_PATH):
            return {}
        try:
            with open(cfg.CONFIG_PATH) as f:
                raw = json.load(f)
            return raw if isinstance(raw, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    # Serializes read-modify-write of ~/.vireo/config.json and the active
    # workspace's config_overrides across the schema-driven settings
    # endpoints (PATCH/DELETE/import). Without this, with per-field autosave
    # and `app.run(threaded=True)` two concurrent requests can read the same
    # snapshot and the later writer drops the earlier change.
    _settings_write_lock = threading.Lock()

    @app.route("/api/settings/global", methods=["PATCH"])
    def api_settings_global_patch():
        """Set a single global config value (validated against SCHEMA)."""
        import config as cfg
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        key = body.get("key")
        if not isinstance(key, str) or key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        try:
            value = schema.validate_value(key, body.get("value"))
        except schema.ValidationError as e:
            return json_error(str(e), status=400)

        with _settings_write_lock:
            raw = _read_raw_config_file()
            schema.set_dotted(raw, key, value)
            cfg.save(raw)
            _settings_post_save_side_effects(cfg.load())
        return jsonify({"ok": True, "key": key, "value": value})

    @app.route("/api/settings/global/<path:key>", methods=["DELETE"])
    def api_settings_global_delete(key):
        """Remove a key from the global config file (reverts to default)."""
        import config as cfg
        import config_schema as schema

        if key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)

        with _settings_write_lock:
            raw = _read_raw_config_file()
            schema.delete_dotted(raw, key)
            cfg.save(raw)
            _settings_post_save_side_effects(cfg.load())
        return jsonify({"ok": True, "key": key})

    def _read_workspace_overrides(db):
        """Return the active workspace's config_overrides as a dict (or {}).

        Coerces non-dict payloads (possible via legacy workspace
        create/update APIs) to ``{}`` so dotted-key mutation in the schema
        write paths can't crash on a malformed override.
        """
        ws = db.get_workspace(db._active_workspace_id)
        if not ws or not ws["config_overrides"]:
            return {}
        try:
            raw = ws["config_overrides"]
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _write_workspace_overrides(db, overrides):
        db.update_workspace(
            db._active_workspace_id,
            config_overrides=overrides if overrides else None,
        )

    @app.route("/api/settings/workspace", methods=["PATCH"])
    def api_settings_workspace_patch():
        """Set a single per-workspace override (validated against SCHEMA)."""
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        key = body.get("key")
        if not isinstance(key, str) or key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        if schema.SCHEMA[key].get("scope") == "global":
            return json_error(
                f"{key!r} is global-only and cannot be overridden per workspace",
                status=400,
            )
        try:
            value = schema.validate_value(key, body.get("value"))
        except schema.ValidationError as e:
            return json_error(str(e), status=400)

        db = _get_db()
        with _settings_write_lock:
            overrides = _read_workspace_overrides(db)
            schema.set_dotted(overrides, key, value)
            _write_workspace_overrides(db, overrides)
        return jsonify({"ok": True, "key": key, "value": value})

    @app.route("/api/settings/workspace/<path:key>", methods=["DELETE"])
    def api_settings_workspace_delete(key):
        """Remove a per-workspace override (the key falls back to global/default)."""
        import config_schema as schema

        if key not in schema.SCHEMA:
            return json_error(f"unknown setting {key!r}", status=400)
        db = _get_db()
        with _settings_write_lock:
            overrides = _read_workspace_overrides(db)
            schema.delete_dotted(overrides, key)
            _write_workspace_overrides(db, overrides)
        return jsonify({"ok": True, "key": key})

    @app.route("/api/settings/export")
    def api_settings_export():
        """Download ~/.vireo/config.json as an attachment.

        Returns the raw user-overrides file (or "{}" if absent), pretty-printed.
        Workspace overrides are not included — they're per-workspace state, not
        global config.
        """
        import datetime as _datetime

        raw = _read_raw_config_file()
        body = json.dumps(raw, indent=2)
        today = _datetime.date.today().isoformat()
        resp = make_response(body)
        resp.headers["Content-Type"] = "application/json"
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="vireo-config-{today}.json"'
        )
        return resp

    @app.route("/api/settings/import", methods=["POST"])
    def api_settings_import():
        """Replace ~/.vireo/config.json with the supplied JSON payload.

        Validates every schema-known leaf key in the payload before writing;
        on any validation failure, returns 400 with a per-key error map and
        leaves the file untouched. Non-schema keys (setup_complete, the
        keyboard_shortcuts subtree, etc.) pass through unchanged so that
        backups round-trip cleanly. Workspace overrides are untouched —
        backups capture global state only.
        """
        import config as cfg
        import config_schema as schema

        body = request.get_json(silent=True) or {}
        raw_text = body.get("json", "")
        if not isinstance(raw_text, str):
            return json_error("body.json must be a string", status=400)
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as e:
            return json_error(f"invalid JSON: {e}", status=400)
        if not isinstance(payload, dict):
            return json_error("payload must be a JSON object", status=400)

        # Iterate the schema directly rather than relying on flatten() — empty
        # objects at schema leaves (e.g. {"classification_threshold": {}}) flatten
        # to nothing and would otherwise be written as-is, replacing a numeric
        # leaf with {} on disk and breaking downstream consumers.
        _MISSING = object()
        errors = {}

        # 1. Reject scalars where a schema-backed object subtree is expected
        #    (e.g. {"pipeline": 5}).
        for prefix in schema.schema_parent_prefixes():
            val = schema.get_dotted(payload, prefix, default=_MISSING)
            if val is _MISSING or isinstance(val, dict):
                continue
            errors[prefix] = f"{prefix} must be a JSON object"

        # 2. For every schema leaf actually present in the payload, reject any
        #    object (empty or otherwise) and run the usual value validation.
        for schema_key in schema.SCHEMA:
            val = schema.get_dotted(payload, schema_key, default=_MISSING)
            if val is _MISSING:
                continue
            if isinstance(val, dict):
                errors[schema_key] = f"{schema_key} must be a JSON scalar, not an object"
                continue
            try:
                coerced = schema.validate_value(schema_key, val)
                schema.set_dotted(payload, schema_key, coerced)
            except schema.ValidationError as e:
                errors[schema_key] = str(e)

        # Structured non-schema keys still need shape validation, otherwise a
        # malformed payload would write through to the file and crash
        # downstream UI consumers that assume a specific shape.
        if "keyboard_shortcuts" in payload:
            # shortcuts.html dereferences `cfg.keyboard_shortcuts.<ctx>.<action>`
            # and assumes a dict tree.
            ks = payload["keyboard_shortcuts"]
            if not isinstance(ks, dict):
                errors["keyboard_shortcuts"] = "keyboard_shortcuts must be a JSON object"
            else:
                for ctx_name, actions in ks.items():
                    if not isinstance(actions, dict):
                        errors[f"keyboard_shortcuts.{ctx_name}"] = (
                            f"keyboard_shortcuts.{ctx_name} must be a JSON object"
                        )
                        continue
                    for action, key_str in actions.items():
                        if not isinstance(key_str, str):
                            errors[f"keyboard_shortcuts.{ctx_name}.{action}"] = (
                                "must be a string"
                            )

        # ingest.recent_destinations is also EXCLUDED from SCHEMA but is a
        # structured value (list[str]). pipeline.html calls
        # `recents.forEach(...)` on it, so a non-list value would crash the
        # pipeline page after a bad import.
        ingest_section = payload.get("ingest")
        if isinstance(ingest_section, dict) and "recent_destinations" in ingest_section:
            recents = ingest_section["recent_destinations"]
            if not isinstance(recents, list):
                errors["ingest.recent_destinations"] = (
                    "ingest.recent_destinations must be a JSON array"
                )
            else:
                for i, item in enumerate(recents):
                    if not isinstance(item, str):
                        errors[f"ingest.recent_destinations[{i}]"] = (
                            "must be a string"
                        )
                        break

        if errors:
            return jsonify({"error": "validation failed", "errors": errors}), 400

        with _settings_write_lock:
            cfg.save(payload)
            _settings_post_save_side_effects(cfg.load())
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
            return json_error("photo_ids required")
        if not all(isinstance(pid, int) for pid in photo_ids):
            return json_error("photo_ids must be a list of integers")

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
            return json_error("No photos found", 404)

        editor = cfg.get("external_editor")
        if editor and not isinstance(editor, str):
            return json_error(
                "external_editor config must be a string path", 500
            )
        editor_path = os.path.expanduser(editor) if editor else ""

        # On macOS, an .app bundle is a directory — execing it raises EACCES.
        # Resolve the bundle when the user gives the parent folder (e.g.
        # /Applications/Adobe Lightroom Classic/) and route through `open -a`.
        app_bundle = None
        if sys.platform == "darwin" and editor_path:
            if editor_path.endswith(".app"):
                app_bundle = editor_path
            elif os.path.isdir(editor_path):
                try:
                    bundles = [
                        entry for entry in sorted(os.listdir(editor_path))
                        if entry.endswith(".app")
                    ]
                except OSError:
                    bundles = []
                if len(bundles) == 1:
                    app_bundle = os.path.join(editor_path, bundles[0])
                elif len(bundles) > 1:
                    return json_error(
                        f"Multiple .app bundles found in {editor_path} "
                        f"({', '.join(bundles)}). "
                        "Set the editor to a specific .app bundle.",
                        500,
                    )
                else:
                    return json_error(
                        f"No .app bundle found in {editor_path}. "
                        "Set the editor to the .app bundle directly.",
                        500,
                    )

        try:
            if app_bundle:
                # `open -a` returns quickly after launching; capture the exit
                # so launch failures surface instead of disappearing silently.
                result = subprocess.run(
                    ["open", "-a", app_bundle] + file_paths,
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "open failed").strip()
                    log.warning("open -a %s failed: %s", app_bundle, err)
                    return json_error(err, 500)
            elif editor_path:
                subprocess.Popen([editor_path] + file_paths)
            elif sys.platform == "darwin":
                result = subprocess.run(
                    ["open"] + file_paths,
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "open failed").strip()
                    log.warning("open %s failed: %s", file_paths, err)
                    return json_error(err, 500)
            elif sys.platform == "win32":
                for fp in file_paths:
                    os.startfile(fp)
            else:
                for fp in file_paths:
                    subprocess.Popen(["xdg-open", fp])
        except Exception as e:
            log.warning("Failed to open external editor: %s", e)
            return json_error(str(e), 500)

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
            # Keep preview_cache table in sync with the filesystem so
            # Settings "Current usage" and eviction don't see phantoms.
            db = _get_db()
            db.conn.execute("DELETE FROM preview_cache")
            db.conn.commit()
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
        # Keep preview_cache rows in sync when previews are deleted directly
        # via this endpoint (stats page). Matches {pid}_{size}.jpg only;
        # legacy {pid}.jpg files have no tracking row to remove.
        preview_rows_removed = 0
        if cache_type == "previews":
            import re
            sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
            db = _get_db()
        for fname in filenames:
            # Prevent path traversal
            safe = os.path.basename(fname)
            fp = os.path.join(cache_dir, safe)
            if os.path.isfile(fp):
                os.remove(fp)
                deleted += 1
                if cache_type == "previews":
                    m = sized_pat.match(safe)
                    if m:
                        db.preview_cache_delete(int(m.group(1)), int(m.group(2)))
                        preview_rows_removed += 1
        if cache_type == "previews" and preview_rows_removed:
            log.info("Removed %d preview_cache rows alongside files", preview_rows_removed)
        log.info("Deleted %d files from %s cache", deleted, cache_type)
        return jsonify({"ok": True, "deleted": deleted})

    @app.route("/api/preview-cache")
    def api_preview_cache():
        """Return counts and totals from the preview_cache table, plus quota."""
        import config as cfg
        db = _get_db()
        count_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()
        total = db.preview_cache_total_bytes()
        quota_mb = cfg.load().get("preview_cache_max_mb", 2048)
        return jsonify({
            "count": count_row["c"],
            "total_size": total,
            "quota_bytes": int(quota_mb) * 1024 * 1024,
        })

    @app.route("/api/preview-cache/clear", methods=["POST"])
    def api_preview_cache_clear():
        """Delete every preview_cache file and row, including legacy and
        untracked files in the previews directory.

        Tracked rows whose on-disk files couldn't be unlinked (e.g. a
        locked or permission-restricted file) are kept so accounting
        and future eviction still reflect the leaked bytes — otherwise
        /api/preview-cache would under-report and eviction would stop
        targeting them.
        """
        import re

        db = _get_db()
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(vireo_dir, "previews")

        count_row = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()
        tracked = count_row["c"]

        # Matches {id}.jpg (legacy /full cache) and {id}_{size}.jpg (current).
        pattern = re.compile(r"^(\d+)(?:_(\d+))?\.jpg$")
        sized_pat = re.compile(r"^(\d+)_(\d+)\.jpg$")
        files_removed = 0
        failed_tracked = []  # (photo_id, size) tuples for sized files we couldn't unlink
        if os.path.isdir(preview_dir):
            for fname in os.listdir(preview_dir):
                if not pattern.match(fname):
                    continue
                try:
                    os.remove(os.path.join(preview_dir, fname))
                    files_removed += 1
                except OSError:
                    m = sized_pat.match(fname)
                    if m:
                        failed_tracked.append((int(m.group(1)), int(m.group(2))))

        if failed_tracked:
            # Stage failed keys in a temp table so the DELETE isn't a giant
            # NOT IN clause that blows past SQLite's default variable limit
            # (~999) with a few hundred unlinkable files. Insert in chunks
            # of 400 pairs (800 bind parameters) for the same reason.
            db.conn.execute(
                "CREATE TEMP TABLE _pc_failed (photo_id INTEGER, size INTEGER)"
            )
            try:
                CHUNK = 400
                for i in range(0, len(failed_tracked), CHUNK):
                    batch = failed_tracked[i:i + CHUNK]
                    placeholders = ",".join(["(?,?)"] * len(batch))
                    flat = [v for pair in batch for v in pair]
                    db.conn.execute(
                        f"INSERT INTO _pc_failed (photo_id, size) VALUES {placeholders}",
                        flat,
                    )
                db.conn.execute(
                    "DELETE FROM preview_cache WHERE (photo_id, size) NOT IN "
                    "(SELECT photo_id, size FROM _pc_failed)"
                )
            finally:
                db.conn.execute("DROP TABLE _pc_failed")
        else:
            db.conn.execute("DELETE FROM preview_cache")
        db.conn.commit()

        remaining = db.conn.execute(
            "SELECT COUNT(*) AS c FROM preview_cache"
        ).fetchone()["c"]
        cleared = tracked - remaining

        return jsonify({
            "cleared": cleared,
            "files_removed": files_removed,
            "failed": len(failed_tracked),
        })

    @app.route("/api/detection-cache/stats")
    def api_detection_cache_stats():
        """Return global detector-cache stats for the settings page.

        `detector_runs` is shared across workspaces, so the numbers do
        not depend on the active workspace.
        """
        db = _get_db()
        return jsonify(db.get_global_detection_stats())

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
        from classifier import _embedding_cache_path, _resolve_model_dir
        from labels import get_saved_labels
        from models import get_models

        # Only BioCLIP-style models use per-label text embeddings. timm models
        # have a fixed class head and never need embedding precomputation, so
        # excluding them here prevents the Settings UI from offering a
        # "Compute" button that would fail with a missing-file error.
        models = [
            m for m in get_models()
            if m["downloaded"] and m.get("model_type", "bioclip") != "timm"
        ]
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
                model_dir = _resolve_model_dir(m["model_str"], m.get("weights_path"))
                cache_path = _embedding_cache_path(labels, m["model_str"], model_dir)
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
            if model.get("model_type", "bioclip") == "timm":
                raise RuntimeError(
                    f"Model {model['name']} has a fixed class head and does "
                    "not use per-label embeddings — nothing to precompute."
                )

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
        seen_paths: set[str] = set()

        def _add_volume(name: str, path: str) -> None:
            if path not in seen_paths and os.path.isdir(path):
                seen_paths.add(path)
                volumes.append({"name": name, "path": path})

        def _scan_dir(vol_dir: str) -> None:
            """List direct children of *vol_dir* as volumes."""
            if os.path.isdir(vol_dir):
                try:
                    entries = sorted(os.listdir(vol_dir))
                except PermissionError:
                    return
                for name in entries:
                    _add_volume(name, os.path.join(vol_dir, name))

        if platform.system() == "Darwin":
            _scan_dir("/Volumes")
        else:
            # /media — flat list of mount points
            _scan_dir("/media")
            # /run/media — systemd convention: /run/media/<user>/<volume>
            run_media = "/run/media"
            if os.path.isdir(run_media):
                try:
                    run_media_entries = sorted(os.listdir(run_media))
                except PermissionError:
                    run_media_entries = []
                for user_dir in run_media_entries:
                    user_path = os.path.join(run_media, user_dir)
                    if os.path.isdir(user_path):
                        try:
                            entries = sorted(os.listdir(user_path))
                        except PermissionError:
                            continue
                        for name in entries:
                            _add_volume(name, os.path.join(user_path, name))
            # /mnt — traditional mount point
            _scan_dir("/mnt")

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
                    "thumb_url": "/api/import/folder-preview/thumbnail?path=" + quote(str(f)),
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

    @app.route("/api/import/new-images-preview", methods=["POST"])
    def api_import_new_images_preview():
        """Preview grid data for a new-images snapshot, matching the
        folder-preview response shape so the same client renderer works."""
        body = request.get_json(silent=True) or {}
        snapshot_id = body.get("snapshot_id")
        if not isinstance(snapshot_id, int):
            return json_error("snapshot_id required", 400)

        db = _get_db()
        if db._active_workspace_id is None:
            abort(404)
        try:
            snap = db.get_new_images_snapshot(snapshot_id)
        except OverflowError:
            snap = None
        if snap is None:
            abort(404)

        # Use only top-level mapped roots (not every auto-registered
        # subfolder the scanner created) so grouping matches the user's
        # source folders — otherwise longest-prefix match picks the
        # deepest descendant and hides which source a file came from.
        from new_images import mapped_roots as _ni_mapped_roots
        root_paths = [r["path"] for r in _ni_mapped_roots(db, db._active_workspace_id)]

        # Build unique display names across roots by taking the shortest
        # trailing path segments that are unique — so /mnt/cardA/DCIM and
        # /mnt/cardB/DCIM become cardA/DCIM and cardB/DCIM rather than
        # colliding on "DCIM". Mirrors folder-preview's disambiguation.
        root_names = {}
        if len(root_paths) > 1:
            parts = [Path(rp).parts for rp in root_paths]
            for depth in range(1, max(len(p) for p in parts) + 1):
                suffixes = [str(Path(*p[-depth:])) for p in parts]
                if len(set(suffixes)) == len(suffixes):
                    for rp, suffix in zip(root_paths, suffixes, strict=True):
                        root_names[rp] = suffix
                    break
            else:
                for rp in root_paths:
                    root_names[rp] = rp
        else:
            for rp in root_paths:
                root_names[rp] = os.path.basename(rp.rstrip("/")) or rp

        roots = sorted(
            [(rp, root_names[rp]) for rp in root_paths],
            key=lambda pn: len(pn[0]),
            reverse=True,
        )

        def _subfolder_for(path):
            for root_path, root_name in roots:
                try:
                    rel = Path(path).parent.relative_to(root_path)
                except ValueError:
                    continue
                rel_str = str(rel)
                return root_name if rel_str == "." else os.path.join(root_name, rel_str)
            return os.path.dirname(path) or "."

        files = []
        type_breakdown = {}
        total_size = 0
        for path in snap["file_paths"]:
            try:
                stat = os.stat(path)
            except OSError:
                continue
            ext = os.path.splitext(path)[1].lower()
            files.append({
                "path": path,
                "filename": os.path.basename(path),
                "subfolder": _subfolder_for(path),
                "size": stat.st_size,
                "extension": ext,
                "mtime": stat.st_mtime,
                "thumb_url": "/api/import/folder-preview/thumbnail?path=" + quote(path),
            })
            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += stat.st_size

        return jsonify({
            "total_count": len(files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @app.route("/api/import/check-duplicates", methods=["POST"])
    def api_import_check_duplicates():
        """Stream duplicate detection results via SSE.

        Accepts {"paths": [...]}, hashes each file, checks against DB,
        and streams batches of duplicate paths back to the client.
        """
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        if not paths:
            return json_error("paths required", 400)

        from scanner import compute_file_hash

        db = _get_db()
        rows = db.conn.execute(
            "SELECT file_hash FROM photos WHERE file_hash IS NOT NULL"
        ).fetchall()
        known_hashes = {r["file_hash"] for r in rows}

        BATCH_SIZE = 20

        def generate():
            total = len(paths)
            duplicate_count = 0
            batch_duplicates = []
            # Also track hashes seen in this run so identical source files
            # (not yet in DB) are reported as duplicates of each other,
            # matching the behaviour of the actual import step.
            seen_hashes = set()

            for checked, path in enumerate(paths, 1):
                try:
                    file_hash = compute_file_hash(path)
                    if file_hash in known_hashes or file_hash in seen_hashes:
                        batch_duplicates.append(path)
                        duplicate_count += 1
                    else:
                        seen_hashes.add(file_hash)
                except OSError:
                    pass  # Skip unreadable/missing files

                if checked % BATCH_SIZE == 0 or checked == total:
                    yield f"data: {json.dumps({'duplicates': batch_duplicates, 'checked': checked, 'total': total})}\n\n"
                    batch_duplicates = []

            yield f"data: {json.dumps({'done': True, 'duplicate_count': duplicate_count, 'checked': total, 'total': total})}\n\n"

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.route("/api/import/collection-preview", methods=["POST"])
    def api_import_collection_preview():
        """Return preview data for photos in a collection."""
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        if not collection_id:
            return json_error("collection_id required", 400)

        db = _get_db()
        photos = db.get_collection_photos(collection_id, page=1, per_page=100000)

        folder_rows = db.conn.execute("SELECT id, path, name FROM folders").fetchall()
        folder_map = {r["id"]: dict(r) for r in folder_rows}

        files = []
        type_breakdown = {}
        total_size = 0

        for p in photos:
            folder = folder_map.get(p["folder_id"], {})
            folder_name = folder.get("name", "Unknown")
            ext = (p["extension"] or "").lower()
            size = p["file_size"] or 0
            folder_path = folder.get("path", "")
            full_path = os.path.join(folder_path, p["filename"]) if folder_path else p["filename"]

            files.append({
                "path": full_path,
                "filename": p["filename"],
                "subfolder": folder_name,
                "size": size,
                "extension": ext,
                "mtime": p["file_mtime"] or 0,
                "thumb_url": f"/thumbnails/{p['id']}.jpg",
                "duplicate": False,
                "photo_id": p["id"],
            })

            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += size

        return jsonify({
            "total_count": len(files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @app.route("/api/import/destination-preview", methods=["POST"])
    def api_import_destination_preview():
        """Preview destination folder structure without copying files."""
        body = request.get_json(silent=True) or {}
        sources = body.get("sources", [])
        destination = body.get("destination", "")
        if not sources:
            return json_error("sources required", 400)
        if not destination:
            return json_error("destination required", 400)
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path", 400)

        from ingest import _is_unsafe_path, preview_destination

        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if folder_template and _is_unsafe_path(folder_template):
            return json_error("folder_template must be a relative path without '..' or backslashes", 400)

        try:
            result = preview_destination(
                sources=sources,
                destination=destination,
                folder_template=folder_template,
                file_types=body.get("file_types", "both"),
                recursive=body.get("recursive", True),
                exclude_paths=body.get("exclude_paths"),
            )
        except ValueError as e:
            return json_error(str(e), 400)
        return jsonify(result)

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

        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        import_untracked(
            db, paths,
            vireo_dir=vireo_dir,
            thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
        )
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

    @app.route("/api/jobs/verify-all-models", methods=["POST"])
    def api_job_verify_all_models():
        """Run SHA256 verification on every installed known model.

        Launches a background job that iterates get_models(), hashes each
        model's LFS files, and writes a .verify_failed sentinel into any
        directory whose files don't match HuggingFace's reported hashes.
        The UI can then show Repair for the bad ones via the existing
        state classifier path.
        """
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            import model_verify

            def progress_cb(msg):
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                        "phase": "Verifying models",
                    },
                )

            results = model_verify.verify_all_models(progress_callback=progress_cb)
            return {
                "verified": len(results),
                "failed": [mid for mid, r in results.items() if not r.ok],
                "ok": [mid for mid, r in results.items() if r.ok],
            }

        job_id = runner.start("verify-models", work, workspace_id=active_ws)
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
            from taxonomy import (
                TAXONOMY_JSON_PATH,
                Taxonomy,
                download_taxonomy,
                populate_taxa_db_from_json,
                seed_informal_groups,
            )

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

            download_taxonomy(TAXONOMY_JSON_PATH, progress_callback=progress_cb)

            bg_db = Database(db_path)
            bg_db.set_active_workspace(active_ws)

            # Populate the SQLite taxa table from the same DWCA data so
            # add_keyword's auto-detect (which queries the DB, not the JSON)
            # can type newly-imported keywords as 'taxonomy' going forward.
            # Roll back and fail the job on error — populate_taxa_db_from_json
            # issues many INSERTs within a single open transaction, and
            # letting the subsequent mark_species_keywords call commit
            # would flush the partial writes onto disk and leave the taxa
            # table silently inconsistent.
            try:
                populate_taxa_db_from_json(
                    bg_db, TAXONOMY_JSON_PATH, progress_callback=progress_cb,
                )
                seed_informal_groups(bg_db)
            except Exception:
                log.error("Post-download taxa DB population failed", exc_info=True)
                bg_db.conn.rollback()
                raise

            # Retype existing keywords that match the new taxonomy so the
            # user sees the effect immediately, without restarting the app.
            # Roll back and fail the job on error: mark_species_keywords
            # accumulates UPDATEs before its own commit, so a mid-flight
            # failure (e.g., transient "database is locked") would leave
            # a pending transaction that a later commit could flush, and
            # reporting success would hide the retype failure from the UI.
            # The download + populate + seed steps already committed, so
            # the user keeps that progress — retrying the download re-runs
            # retype for free (it's idempotent).
            progress_cb("Retyping existing keywords...")
            try:
                tax = Taxonomy(TAXONOMY_JSON_PATH)
                updated = bg_db.mark_species_keywords(tax)
                log.info("Retyped %d existing keywords as taxonomy after download", updated)
            except Exception:
                log.error("Post-download keyword retype failed", exc_info=True)
                bg_db.conn.rollback()
                raise
            return {"ok": True, "keywords_retyped": updated}

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
            # timm models have a fixed class head — no per-label embeddings.
            if (
                active_model
                and active_model["downloaded"]
                and active_model.get("model_type", "bioclip") != "timm"
            ):
                try:
                    from classifier import _embedding_cache_path, _resolve_model_dir

                    model_dir = _resolve_model_dir(
                        active_model["model_str"], active_model.get("weights_path")
                    )
                    cache_path = _embedding_cache_path(
                        list(set(species)), active_model["model_str"], model_dir
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

        # Use the current-fingerprint helper so a photo with cached
        # predictions from multiple label sets doesn't prefill iNat with
        # a species from a stale label set. Apply the workspace-effective
        # detector_confidence floor so we never prefill a taxon from a
        # detection the UI threshold hides.
        min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        pred = db.get_top_prediction_for_photo(
            photo_id, min_detector_confidence=min_conf,
        )

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

        # Use overrides from request, or fall back to DB data. The helper
        # picks the highest-confidence prediction from the CURRENT
        # fingerprint, scoped to the active workspace, and respecting the
        # workspace-effective detector_confidence floor — submitting a
        # stale or below-threshold taxon to iNaturalist is permanent and
        # not easily reversible.
        min_conf = db.get_effective_config(user_cfg).get(
            "detector_confidence", 0.2
        )
        pred = db.get_top_prediction_for_photo(
            photo_id, min_detector_confidence=min_conf,
        )

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
        # Resolve the workspace-effective detector_confidence floor once for
        # the whole batch — read-time thresholding means below-threshold
        # detections should never seed an iNat submission.
        min_conf = db.get_effective_config(user_cfg).get(
            "detector_confidence", 0.2
        )
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

            # Current-fingerprint + workspace-scoped top prediction,
            # respecting the active detector_confidence floor — avoids
            # submitting a stale-label-set or now-hidden taxon to iNaturalist.
            pred = db.get_top_prediction_for_photo(
                photo_id, min_detector_confidence=min_conf,
            )

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
        """Return system information: ONNX Runtime, hardware."""
        import platform

        info = {
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

        # "installed" requires both module AND weights — module-only
        # lets classify silently fall back to full-image classification.
        try:
            from detector import MEGADETECTOR_ONNX_PATH

            if os.path.isfile(MEGADETECTOR_ONNX_PATH):
                size_mb = round(os.path.getsize(MEGADETECTOR_ONNX_PATH) / 1024 / 1024, 1)
                info["megadetector"] = "installed"
                info["megadetector_detail"] = "MegaDetector V6 (YOLOv9-c) — subject detection for crop-based classification"
                info["megadetector_weights"] = "downloaded"
                info["megadetector_weights_path"] = MEGADETECTOR_ONNX_PATH
                info["megadetector_weights_size"] = f"{size_mb} MB"
            else:
                info["megadetector"] = "weights_missing"
                info["megadetector_detail"] = "Weights not downloaded — subject detection disabled until the MegaDetector V6 ONNX model is downloaded from the pipeline models page."
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
            data_path = model_path + ".data"
            status = "not downloaded"
            size = None
            # DINOv2 uses external-data ONNX: model.onnx is just the ~1 MB graph;
            # the real weights live in a model.onnx.data sidecar. Both must be
            # present for the model to load.
            if os.path.isfile(model_path) and os.path.isfile(data_path):
                total_bytes = os.path.getsize(model_path) + os.path.getsize(data_path)
                size = round(total_bytes / 1024 / 1024, 1)
                status = "downloaded"
            elif os.path.isfile(model_path) or os.path.isfile(data_path):
                status = "incomplete"
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
                "files": ["model.onnx", "model.onnx.data"],
            },
            "vit-b14": {
                "subfolder": "dinov2-vit-b14",
                "files": ["model.onnx", "model.onnx.data"],
            },
            "vit-l14": {
                "subfolder": "dinov2-vit-l14",
                "files": ["model.onnx", "model.onnx.data"],
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

    def _build_scan_work(roots, incremental, active_ws):
        """Build the background work function for a scan job.

        Shared by ``POST /api/jobs/scan`` and
        ``POST /api/folders/<id>/rescan`` so per-folder rescans reuse the
        same scan + thumbnail pipeline as a full scan.

        ``roots`` may be a single path string (back-compat, one root) or a
        list of paths. When multiple roots are given they are scanned
        **serially** inside this single job -- that's the whole point of
        this wrapper: parallel scan jobs used to fight for the SQLite
        writer lock, so we now process roots one after another. A failure
        on one root does not abort the others; the error is recorded and
        the job ends in ``"failed"`` (mixed-outcome rollup convention).
        """
        import config as cfg

        runner = app._job_runner

        # Back-compat: accept a bare string in addition to a list.
        if isinstance(roots, str):
            roots_list = [roots]
        else:
            roots_list = list(roots)

        def work(job):
            from scanner import scan as do_scan

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            # Check folder health before scanning to prevent duplicate imports
            thread_db.check_folder_health()

            # Accumulator so multi-root progress doesn't rewind at each
            # root boundary. scanner.scan() reports (current, total) local
            # to its invocation; we fold those into cumulative counters
            # that the SSE/status stream reads.
            # Track both the last reported *processed* count and the
            # last reported *total* for the current root. On root
            # boundary we advance the cumulative baseline by the
            # processed count (not the planned total) so a root that
            # fails mid-scan doesn't inflate the baseline with phantom
            # files the next root would start above.
            scan_acc = {"prior": 0, "last_current": 0, "last_total": 0}

            def progress_cb(current, total):
                scan_acc["last_current"] = current
                scan_acc["last_total"] = total
                cum_current = scan_acc["prior"] + current
                cum_total = scan_acc["prior"] + total
                job["progress"]["current"] = cum_current
                job["progress"]["total"] = cum_total
                runner.update_step(
                    job["id"], "scan",
                    progress={"current": cum_current, "total": cum_total},
                )
                runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": cum_current,
                        "total": cum_total,
                        "current_file": job["progress"].get("current_file", ""),
                        "rate": round(
                            cum_current / max(time.time() - job["_start_time"], 0.01), 1
                        ),
                        "phase": "Scanning photos",
                    },
                )

            def advance_scan_acc():
                # Use processed count, not planned total — a root that
                # raised mid-scan will have last_current < last_total,
                # and starting the next root above the actual processed
                # count would overreport photos indexed.
                scan_acc["prior"] += scan_acc["last_current"]
                scan_acc["last_current"] = 0
                scan_acc["last_total"] = 0

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

            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

            # Per-root failures are caught and recorded rather than
            # re-raised so a failure on root A doesn't prevent root B
            # from scanning. Any failure flips the job to "failed" at
            # the end (mixed-outcome rollup).
            #
            # Track roots by failure class so the rollup below can
            # distinguish "this root's scan raised" (no photos indexed
            # — thumbnails can skip) from "this root's scan succeeded
            # but cache invalidation raised" (photos DID get indexed —
            # thumbnails must still run). Using len(root_errors) alone
            # double-counts roots that hit both failure classes and
            # misclassifies cache-only failures as scan failures.
            root_errors = []
            scan_failed_roots = set()
            cache_failed_roots = set()
            for idx, root in enumerate(roots_list, 1):
                phase = (
                    f"Scanning root {idx} of {len(roots_list)}: {root}"
                    if len(roots_list) > 1
                    else "Scanning photos"
                )
                runner.push_event(job["id"], "progress", {
                    "phase": phase,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": phase,
                    "rate": 0,
                })
                try:
                    do_scan(
                        root, thread_db,
                        progress_callback=progress_cb,
                        incremental=incremental,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        status_callback=status_cb,
                        vireo_dir=vireo_dir,
                        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                    )
                except Exception as exc:
                    log.exception("Scan failed for root %s", root)
                    scan_failed_roots.add(root)
                    msg = f"[{root}] {exc}"
                    root_errors.append(msg)
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                finally:
                    # scanner.scan commits photo rows incrementally, so
                    # even a mid-scan failure can leave DB state that
                    # invalidates cached new-image counts. A failure
                    # here must surface: the shared cache has a 5-min
                    # TTL, so users would see stale "new images" counts
                    # with no job-level failure signal if we swallowed
                    # these errors. Keep the try/except so we still
                    # advance scan_acc and try the remaining roots,
                    # but record the failure into root_errors so the
                    # job is flagged failed at the rollup below.
                    try:
                        _invalidate_new_images_after_scan(thread_db, root)
                    except Exception as cache_exc:
                        log.exception(
                            "Failed to invalidate new-image cache for %s", root,
                        )
                        cache_failed_roots.add(root)
                        cache_msg = (
                            f"[{root}] cache invalidation failed "
                            f"after scan: {cache_exc}"
                        )
                        root_errors.append(cache_msg)
                        if cache_msg not in job["errors"]:
                            job["errors"].append(cache_msg)
                    advance_scan_acc()

            # Use cumulative processed count, not planned total — on a
            # clean run they're equal; on mixed-outcome runs "current"
            # reflects the actual photos indexed while "total" includes
            # planned-but-unprocessed files from the failed root(s).
            photo_count = job["progress"].get("current", 0)
            # Unique roots that hit any failure class. Counting unique
            # roots (not error entries) avoids inflating the "N of M"
            # summary when a single root raises in both scan and cache
            # invalidation.
            failed_root_count = len(scan_failed_roots | cache_failed_roots)
            if root_errors:
                scan_summary = (
                    f"{photo_count} photos ({failed_root_count} of "
                    f"{len(roots_list)} root"
                    f"{'s' if len(roots_list) != 1 else ''} failed)"
                )
                runner.update_step(
                    job["id"], "scan", status="failed", summary=scan_summary,
                    error=root_errors[0], error_count=len(root_errors),
                )
            else:
                runner.update_step(
                    job["id"], "scan", status="completed",
                    summary=f"{photo_count} photos",
                )
            # Skip the thumbnail phase when EVERY requested root's scan
            # raised. generate_all() walks the whole library looking
            # for missing thumbnails — running it after a total scan
            # failure does a long, unrelated pass and delays the
            # failure feedback the user actually needs. When at least
            # one root's scan succeeded we still run thumbs so those
            # newly-indexed photos get covered. Cache-invalidation
            # failures do NOT gate this decision: the scan for that
            # root did produce indexed photos that need thumbnails.
            all_roots_failed = (
                bool(roots_list) and len(scan_failed_roots) == len(roots_list)
            )

            if all_roots_failed:
                log.info(
                    "All %d scan root(s) failed; skipping thumbnail phase",
                    len(roots_list),
                )
                runner.update_step(
                    job["id"], "thumbnails", status="skipped",
                    summary="skipped (all scan roots failed)",
                )
                thumb_result = None
            else:
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
                    thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=thumb_cb,
                    vireo_dir=vireo_dir,
                )
                from thumbnails import format_summary as thumb_summary
                runner.update_step(job["id"], "thumbnails", status="completed",
                                   summary=thumb_summary(thumb_result))

            # Mixed-outcome rollup: any failed root => job is "failed".
            # JobRunner._run_job dedupes job["errors"] by exact string
            # match. Raise the first per-root message (already recorded
            # above) so no extra aggregate entry is appended — that
            # would inflate error_count in job/history output. The
            # "N of M roots failed" context is already visible via the
            # scan step's summary and error_count set above.
            if root_errors:
                raise RuntimeError(root_errors[0])

            return {"photos_indexed": photo_count, "thumbnails": thumb_result}

        return work

    @app.route("/api/jobs/scan", methods=["POST"])
    def api_job_scan():
        """Queue a scan job.

        Body accepts either:
          * ``{"root": "/path"}`` -- single root (back-compat).
          * ``{"roots": ["/a", "/b", ...]}`` -- multiple roots, scanned
            serially inside a single job. Multi-root support avoids the
            SQLite writer-lock contention that used to happen when the
            UI enqueued one job per root (PR #634 added retry/backoff
            as defense-in-depth; this is the root-cause fix).
        """
        body = request.get_json(silent=True) or {}
        incremental = body.get("incremental", False)

        # Normalize inputs. Prefer the explicit plural form when both are
        # provided; a caller who sends ``roots`` has opted into the new API.
        if "roots" in body:
            roots_in = body.get("roots")
            if not isinstance(roots_in, list) or not roots_in:
                return json_error("roots must be a non-empty list")
            roots_list = [str(r) for r in roots_in if r]
            if not roots_list:
                return json_error("roots must be a non-empty list")
        else:
            root = body.get("root", "")
            if not root:
                return json_error("root path required")
            roots_list = [root]

        for r in roots_list:
            if not os.path.isdir(r):
                return json_error(f"directory not found: {r}")

        # Remember scan roots (skip temp directories from tests)
        import tempfile

        import config as cfg

        tmp_prefix = os.path.realpath(tempfile.gettempdir())
        user_cfg = cfg.load()
        saved_roots = user_cfg.get("scan_roots", [])
        changed = False
        for r in roots_list:
            if os.path.realpath(r).startswith(tmp_prefix):
                continue
            if r not in saved_roots:
                saved_roots.insert(0, r)
                changed = True
        if changed:
            user_cfg["scan_roots"] = saved_roots
            cfg.save(user_cfg)

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        work = _build_scan_work(roots_list, incremental, active_ws)

        job_config = {"roots": roots_list, "incremental": incremental}
        # Back-compat: keep ``root`` in config when exactly one was given,
        # so existing consumers (history viewers, etc.) still find it.
        if len(roots_list) == 1:
            job_config["root"] = roots_list[0]

        job_id = runner.start(
            "scan", work, config=job_config, workspace_id=active_ws,
        )
        return jsonify({"job_id": job_id})

    @app.route("/api/folders/<int:folder_id>/rescan", methods=["POST"])
    def api_folder_rescan(folder_id):
        """Queue a scan job scoped to the given folder's path.

        Body (optional): {"incremental": bool}
        Returns: {"job_id": "scan-..."} on success; 404 if the folder id
        is unknown or not linked to the active workspace.
        """
        body = request.get_json(silent=True) or {}
        incremental = bool(body.get("incremental", False))
        db = _get_db()
        folder = db.get_folder(folder_id)
        if not folder:
            return json_error("folder not found", 404)
        # Folders are global but scans emit workspace-scoped data (predictions,
        # pending_changes). Reject rescans of folders the active workspace has
        # no claim on — otherwise a stale UI or crafted request could pollute
        # this workspace with scan output from an unrelated folder, and
        # add_folder's auto-link would silently attach it.
        active_ws = db._active_workspace_id
        linked = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (active_ws, folder_id),
        ).fetchone()
        if not linked:
            return json_error("folder not found", 404)
        root = folder["path"]
        if not os.path.isdir(root):
            return json_error(f"folder path no longer exists: {root}")
        runner = app._job_runner

        work = _build_scan_work(root, incremental, active_ws)

        job_id = runner.start(
            "scan", work,
            config={
                "root": root,
                "incremental": incremental,
                "folder_id": folder_id,
            },
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
            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
            return generate_all(
                thread_db, app.config["THUMB_CACHE_DIR"], progress_callback=progress_cb,
                vireo_dir=vireo_dir,
            )

        job_id = runner.start("thumbnails", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/duplicates/scan", methods=["POST"])
    def api_duplicates_scan():
        """Start a background duplicate-detection job.

        Returns immediately with the job id. The job walks every file_hash
        group with 2+ rows (both unresolved AND already-auto-resolved
        groups) and proposes a winner/losers per group. Resolved groups
        are flagged ``status='resolved'`` so the UI can surface them in a
        separate section for disk cleanup.

        The UI polls /api/jobs/<id> to fetch the proposals from job.result
        and lets the user apply unresolved groups via /api/duplicates/apply
        or trash already-resolved loser files via
        /api/duplicates/delete-loser-files.
        """
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            from duplicate_scan import run_duplicate_scan
            thread_db = Database(db_path)
            if active_ws is not None:
                thread_db.set_active_workspace(active_ws)
            try:
                return run_duplicate_scan(job, thread_db, include_resolved=True)
            finally:
                thread_db.conn.close()

        job_id = runner.start("duplicate-scan", work, workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/duplicates/apply", methods=["POST"])
    def api_duplicates_apply():
        """Apply resolver decisions for the given list of file hashes.

        Body: {"hashes": ["<hash>", ...]}. For each hash we look up every
        non-rejected photo sharing it and hand that set to
        apply_duplicate_resolution, which picks a winner via the pure
        resolver and flags the losers as rejected. Returns the total number
        of photos rejected across all hashes.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("hashes required")
        hashes = body.get("hashes")
        if not isinstance(hashes, list) or not hashes:
            return json_error("hashes required")

        for h in hashes:
            if not isinstance(h, str) or not h:
                return json_error("hashes must be a list of non-empty strings")

        db = _get_db()
        total_rejected = 0
        for h in hashes:
            rows = db.conn.execute(
                "SELECT id FROM photos "
                "WHERE file_hash = ? AND flag != 'rejected'",
                (h,),
            ).fetchall()
            if len(rows) < 2:
                continue
            result = db.apply_duplicate_resolution([r["id"] for r in rows])
            total_rejected += result.get("rejected", 0)
        return jsonify({"rejected_count": total_rejected})

    @app.route("/api/folders/reveal", methods=["POST"])
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

        db = _get_db()
        # Normalize on read so legacy/relocated rows stored with a
        # trailing separator still match bucket-UI paths derived from
        # ``os.path.dirname(...)`` — same trap bulk_resolve_by_folder
        # patched. JOIN workspace_folders so paths from other workspaces
        # are treated as unknown (without this gate the endpoint would
        # be a cross-workspace path oracle / open-action gadget).
        norm_paths = [os.path.normpath(p) for p in paths]
        all_rows = db.conn.execute(
            """SELECT f.path FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?""",
            (db._active_workspace_id,),
        ).fetchall()
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
                                          timeout=5, check=False)
                elif sys.platform.startswith("win"):
                    # Folder reveal opens the folder itself (no /select,)
                    # so the user sees its contents.
                    proc = subprocess.run(["explorer", path],
                                          timeout=5, check=False)
                else:
                    # xdg-open doesn't honor `--`; abspath guarantees a
                    # leading slash so a crafted leading-dash path can't
                    # be parsed as a flag.
                    proc = subprocess.run(
                        ["xdg-open", os.path.abspath(path)],
                        timeout=5, check=False,
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

    @app.route("/api/duplicates/bulk-resolve", methods=["POST"])
    def api_duplicates_bulk_resolve():
        """Force-resolve a batch of duplicate groups by keeping the photo
        whose folder matches ``keep_folder``.

        Body: ``{"file_hashes": [str, ...], "keep_folder": str}``. For each
        hash, the photo in ``keep_folder`` becomes the kept winner; every
        other non-rejected photo sharing the hash becomes rejected, with
        rating/keywords merged onto the winner.

        Returns ``{"ok": True, "resolved_count": int, "resolved":
        [{"file_hash", "winner_id", "loser_ids"}], "skipped":
        [{"file_hash", "reason"}]}``. ``loser_ids`` is surfaced so the UI
        can chain into ``/api/duplicates/delete-loser-files`` when the
        user opted in to immediate trash.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("file_hashes and keep_folder required")
        file_hashes = body.get("file_hashes")
        keep_folder = body.get("keep_folder")
        if not isinstance(file_hashes, list) or not file_hashes:
            return json_error("file_hashes required")
        if not isinstance(keep_folder, str) or not keep_folder:
            return json_error("keep_folder required")
        for h in file_hashes:
            if not isinstance(h, str) or not h:
                return json_error("file_hashes must be a list of non-empty strings")

        db = _get_db()
        result = db.bulk_resolve_by_folder(file_hashes, keep_folder)
        return jsonify({
            "ok": True,
            "resolved_count": len(result["resolved"]),
            "resolved": result["resolved"],
            "skipped": result["skipped"],
        })

    @app.route("/api/duplicates/delete-loser-files", methods=["POST"])
    def api_duplicates_delete_loser_files():
        """Move duplicate loser files to OS Trash and remove their DB rows.

        Body: {"photo_ids": [int, ...]}. For each id we require:
          - the row's flag is 'rejected' (already auto-resolved or
            user-applied), AND
          - at least one OTHER photo with the same ``file_hash`` is NOT
            rejected (the kept "winner" anchor that makes this row a
            duplicate-loser rather than an unrelated rejection).

        Validating both conditions prevents this endpoint from being misused
        to trash files for arbitrary rejected photos (e.g. a photo the user
        manually rejected for non-duplicate reasons).

        After a successful trash we also delete the loser's photo row (and
        its cached thumbnail / preview / working-copy files). Without that,
        ``/api/duplicates/disk-cleanup-summary`` would keep reporting the
        same count forever — the summary predicate can't cheaply tell that
        the on-disk file has been removed (stat'ing every loser path on a
        slow network volume would make the banner poll expensive). Deleting
        the row makes the count correct without a stat. The keywords/rating
        were merged onto the winner during ``apply_duplicate_resolution``,
        so nothing of value is lost. If the user later restores the file
        from Trash and re-scans, the auto-resolve hook re-creates the row
        and the cycle is idempotent.

        Returns ``{trashed: N, skipped: [{id, reason}, ...],
        failed: [{id, path, error}, ...]}``.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("photo_ids required")
        photo_ids = body.get("photo_ids")
        if not isinstance(photo_ids, list) or not photo_ids:
            return json_error("photo_ids required")
        for pid in photo_ids:
            # ``bool`` is a subclass of ``int`` in Python, so a bare
            # ``isinstance(pid, int)`` would accept ``True``/``False`` as
            # valid ids — and ``True`` would then be treated as photo id 1.
            # Reject booleans explicitly so ``{"photo_ids": [true]}`` can't
            # trick the endpoint into trashing whichever rejected row
            # happens to have id 1.
            if isinstance(pid, bool) or not isinstance(pid, int):
                return json_error("photo_ids must be a list of integers")

        db = _get_db()
        # Chunk the lookup SELECT — bulk cleanup actions may hand us thousands
        # of ids at once, and SQLite builds with the legacy 999-parameter cap
        # would otherwise fail before any cleanup runs.
        rows_by_id = {}
        for chunk in _chunked(photo_ids):
            placeholders = ",".join("?" * len(chunk))
            chunk_rows = db.conn.execute(
                f"""SELECT p.id, p.flag, p.file_hash, p.filename,
                           f.path AS folder_path
                    FROM photos p
                    LEFT JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders})""",
                chunk,
            ).fetchall()
            for r in chunk_rows:
                rows_by_id[r["id"]] = r

        # One query per distinct hash to find kept-row anchors. Cheap because
        # the hash column is indexed and a typical bulk action shares hashes
        # across many photo_ids only when the user clicks "trash all losers"
        # for one library — still a small number of distinct hashes.
        hashes = {r["file_hash"] for r in rows_by_id.values() if r["file_hash"]}
        anchored_hashes = set()
        for h in hashes:
            anchor = db.conn.execute(
                "SELECT 1 FROM photos "
                "WHERE file_hash = ? AND flag != 'rejected' LIMIT 1",
                (h,),
            ).fetchone()
            if anchor is not None:
                anchored_hashes.add(h)

        trashed = 0
        trashed_pids = []
        skipped = []
        failed = []
        for pid in photo_ids:
            row = rows_by_id.get(pid)
            if row is None:
                skipped.append({"id": pid, "reason": "photo not found"})
                continue
            if row["flag"] != "rejected":
                skipped.append({"id": pid, "reason": "photo is not rejected"})
                continue
            if not row["file_hash"] or row["file_hash"] not in anchored_hashes:
                # No kept row shares this hash — refuse to trash. Treat as
                # "not a duplicate loser" so the user can't accidentally use
                # this endpoint to delete files for unrelated rejected rows.
                skipped.append({"id": pid, "reason": "no duplicate winner exists"})
                continue
            filepath = os.path.join(row["folder_path"] or "", row["filename"] or "")
            file_existed = os.path.isfile(filepath)
            if not file_existed:
                # File was removed outside Vireo (e.g. user trashed in Finder).
                # Drop the orphan DB row anyway so the summary count drops —
                # without this, manually-cleaned losers would also "report
                # forever". The "skipped" status still surfaces the no-op to
                # the caller for accurate reporting.
                skipped.append({"id": pid, "reason": "file already missing"})
                trashed_pids.append(pid)
                continue
            try:
                from send2trash import send2trash as _trash
                _trash(filepath)
                trashed += 1
                trashed_pids.append(pid)
            except Exception:
                log.debug("send2trash failed for %s, trying Finder", filepath)
                try:
                    _trash_via_finder(filepath)
                    trashed += 1
                    trashed_pids.append(pid)
                except Exception as e:
                    log.warning("Trash failed for %s", filepath, exc_info=True)
                    failed.append({"id": pid, "path": filepath, "error": str(e)})

        # Drop DB rows + cached derivatives for every photo whose file is now
        # gone. Chunked so ``delete_photos``' five internal IN-clause queries
        # can't trip the SQLite parameter cap on large bulk actions; without
        # chunking, a 1000+ id request would raise OperationalError on legacy
        # builds AFTER files were already trashed, leaving the DB inconsistent.
        if trashed_pids:
            try:
                all_files = []
                for chunk in _chunked(trashed_pids):
                    result = db.delete_photos(chunk)
                    all_files.extend(result.get("files", []))
                _cleanup_cached_files_for_deleted_photos(all_files)
            except Exception:
                # Files are already in Trash; if the row delete fails we
                # surface a 500 so the caller knows reconciliation is
                # incomplete. Without raising, the summary count would stay
                # inflated and the caller would have no signal that the
                # cleanup is half-done.
                log.exception(
                    "DB row delete failed after trashing %d files", len(trashed_pids),
                )
                return jsonify({
                    "ok": False,
                    "error": "trashed files but failed to clean up DB rows",
                    "trashed": trashed,
                    "skipped": skipped,
                    "failed": failed,
                }), 500

        return jsonify({
            "ok": True,
            "trashed": trashed,
            "skipped": skipped,
            "failed": failed,
        })

    @app.route("/api/duplicates/last-scan", methods=["GET"])
    def api_duplicates_last_scan():
        """Return the most recent completed duplicate-scan's result.

        The /duplicates page only holds proposals in JS memory, so
        navigating away and back used to require a full rescan. This
        lets the page restore prior results from ``job_history`` instead.

        Library-wide on purpose: duplicate detection itself ignores
        workspace scope (photos are global), so the result of any
        completed scan is valid for any active workspace — even though
        the row carries the triggering workspace's id.

        Response: ``{found: false}`` or
        ``{found: true, job_id, started_at, finished_at, result}``.
        """
        db = _get_db()
        row = db.conn.execute(
            """SELECT id, started_at, finished_at, result
                 FROM job_history
                WHERE type = 'duplicate-scan'
                  AND status = 'completed'
                  AND result IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1"""
        ).fetchone()
        if row is None:
            return jsonify({"found": False})
        try:
            result = json.loads(row["result"])
        except (json.JSONDecodeError, TypeError):
            return jsonify({"found": False})
        return jsonify({
            "found": True,
            "job_id": row["id"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "result": result,
        })

    @app.route("/api/duplicates/disk-cleanup-summary", methods=["GET"])
    def api_duplicates_disk_cleanup_summary():
        """Return counts of duplicate-loser files that may still be on disk.

        Body: ``{count: int, total_size: int, file_hashes: [str, ...]}``.

        Powers the navbar banner that surfaces the volume of cleanup
        available — without it, auto-resolved duplicates from scan are
        invisible to the user.

        ``count`` is the number of rejected photo rows whose hash is also
        held by a non-rejected row (i.e. duplicate losers, not unrelated
        rejections). ``total_size`` is the sum of their stored ``file_size``
        — a best-effort estimate; we do NOT stat each path here because
        slow network volumes (e.g. SMB) would make this endpoint expensive
        on every banner poll. The bulk-trash endpoint validates each file
        exists before trashing.
        """
        db = _get_db()
        row = db.conn.execute(
            """
            SELECT COUNT(*) AS n, COALESCE(SUM(file_size), 0) AS total_bytes
            FROM photos p
            WHERE p.flag = 'rejected'
              AND p.file_hash IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM photos q
                  WHERE q.file_hash = p.file_hash AND q.flag != 'rejected'
              )
            """
        ).fetchone()
        return jsonify({
            "count": row["n"],
            "total_size": row["total_bytes"],
        })

    @app.route("/api/jobs/previews", methods=["POST"])
    def api_job_previews():
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            import contextlib

            import config as cfg
            from image_loader import get_canonical_image_path, load_image

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
            # Use workspace-effective config so per-workspace preview_max_size
            # overrides are honored — otherwise precompute warms the wrong
            # size and /photos/<id>/full (which uses workspace overrides) still
            # misses on first view.
            effective = thread_db.get_effective_config(cfg.load())
            raw_size = effective.get("preview_max_size")
            if raw_size == 0:
                # "Full resolution" — /full redirects to /original so
                # there's no size-suffixed file to warm. Skip precompute
                # rather than produce untracked {id}.jpg files.
                job["_start_time"] = time.time()
                return {"generated": 0, "skipped": 0, "total": 0,
                        "note": "skipped (preview_max_size=0)"}
            max_size = int(raw_size or 1920)

            preview_quality = effective.get("preview_quality", 90)
            preview_dir = os.path.join(vireo_dir, "previews")
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
                cache_path = os.path.join(preview_dir, f'{photo["id"]}_{max_size}.jpg')
                if os.path.exists(cache_path):
                    skipped += 1
                    # Adopt any untracked file so precompute output is
                    # visible to eviction and /api/preview-cache.
                    # Best-effort: photo may be deleted mid-job (FK error).
                    with contextlib.suppress(Exception):
                        if not thread_db.preview_cache_get(photo["id"], max_size):
                            thread_db.preview_cache_insert(
                                photo["id"], max_size, os.path.getsize(cache_path),
                            )
                else:
                    canonical = get_canonical_image_path(photo, vireo_dir, folders)
                    img = load_image(canonical, max_size=max_size)
                    if img:
                        img.save(cache_path, format="JPEG", quality=preview_quality)
                        with contextlib.suppress(Exception):
                            thread_db.preview_cache_insert(
                                photo["id"], max_size, os.path.getsize(cache_path),
                            )
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

            # Run a single eviction pass at the end so the batch doesn't
            # fsync after every photo.
            evict_preview_cache_if_over_quota(thread_db, vireo_dir)

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
        from ingest import _is_unsafe_path
        if folder_template and _is_unsafe_path(folder_template):
            return json_error("folder_template must be a relative path without '..' or backslashes")

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

    # -- Export job route --

    @app.route("/api/jobs/export", methods=["POST"])
    def api_job_export():
        """Export selected photos to a destination directory."""
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        destination = body.get("destination", "")
        naming_template = body.get("naming_template", "{original}")
        max_size = body.get("max_size")
        quality = body.get("quality", 92)

        if not raw_ids:
            return json_error("photo_ids required")
        try:
            photo_ids = [int(pid) for pid in raw_ids]
        except (ValueError, TypeError):
            return json_error("photo_ids must be integers")
        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        db = _get_db()
        runner = app._job_runner
        active_ws = db._active_workspace_id

        # Filter to only photos visible in the active workspace,
        # preserving the caller's original ordering.
        placeholders = ",".join("?" for _ in photo_ids)
        visible = db.conn.execute(
            f"""SELECT p.id FROM photos p
                JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
            [active_ws] + list(photo_ids),
        ).fetchall()
        visible_set = {r["id"] for r in visible}
        photo_ids = [pid for pid in photo_ids if pid in visible_set]
        if not photo_ids:
            return json_error("no exportable photos in current workspace")
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        effective_cfg = db.get_effective_config(cfg.load())
        wc_max_size = effective_cfg.get("working_copy_max_size", 4096)
        # Pass the configured darktable output dir so export prefers the
        # perfected render over a fresh libraw decode of the RAW.
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

        def work(job):
            from export import export_photos

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
                    "phase": "Exporting photos",
                })

            return export_photos(
                db=thread_db,
                vireo_dir=vireo_dir,
                photo_ids=photo_ids,
                destination=destination,
                options={
                    "naming_template": naming_template,
                    "max_size": max_size,
                    "quality": quality,
                    "working_copy_max_size": wc_max_size,
                    "developed_dir": developed_dir,
                },
                progress_cb=progress_cb,
            )

        job_id = runner.start(
            "export", work,
            config={
                "photo_ids": photo_ids,
                "destination": destination,
                "naming_template": naming_template,
            },
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

        import config as cfg
        effective_cfg = _get_db().get_effective_config(cfg.load())
        developed_dir = effective_cfg.get("darktable_output_dir", "") or ""

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
                developed_dir=developed_dir,
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
            from ingest import _is_unsafe_path
            if folder_template and _is_unsafe_path(folder_template):
                return json_error("folder_template must be a relative path without '..' or backslashes")

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
            # restrict_dirs narrows the post-ingest scan to just the subfolders
            # that received files, instead of walking the full destination
            # tree. Populated in the copy branch from ingest_result's
            # copied_paths (parent dirs) and duplicate_folders. Left as None
            # for copy=false so scan-in-place keeps its original full-tree
            # behavior.
            restrict_dirs = None

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
                duplicate_folders = ingest_result.get("duplicate_folders", [])
                scan_target = destination

                # Build restrict_dirs from the folders ingest actually touched
                # so the post-ingest scan doesn't re-walk the entire
                # destination tree. Without this, importing ~2k RAWs into a
                # populated library caused scanner.scan to enumerate tens of
                # thousands of already-indexed files (observed: 59k). Mirrors
                # the same pattern in pipeline_job.py. Only paths under the
                # normalized destination are included; ".." tricks cannot
                # escape. If nothing was copied and no duplicate folders were
                # reported, restrict_dirs stays an empty list — scanner.scan
                # then has no directories to enumerate, which matches intent
                # (there is nothing new to index).
                dest_normalized = Path(os.path.normpath(destination))

                def _under_destination(path_str):
                    try:
                        return Path(os.path.normpath(path_str)).is_relative_to(
                            dest_normalized
                        )
                    except ValueError:
                        return False

                restrict_set = set()
                for cp in copied_paths:
                    parent = str(Path(cp).parent)
                    if _under_destination(parent):
                        restrict_set.add(parent)
                for folder in duplicate_folders:
                    if _under_destination(folder):
                        restrict_set.add(folder)
                restrict_dirs = sorted(restrict_set)

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

            vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
            try:
                # copy=false: scan_target is the source and restrict_dirs is
                #   None, so scanner walks the full source tree (unchanged).
                # copy=true: scan_target is the destination (folder hierarchy
                #   root, for parent-folder chain creation), but restrict_dirs
                #   narrows enumeration to only the subfolders ingest wrote
                #   into. An empty list means "nothing new to scan" — a no-op
                #   inside scanner.scan.
                do_scan(
                    scan_target, thread_db,
                    progress_callback=scan_cb,
                    skip_paths=exclude_paths or None,
                    vireo_dir=vireo_dir,
                    thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                    restrict_dirs=restrict_dirs,
                )
            finally:
                # scanner.scan commits photo rows incrementally, so even a mid-scan
                # failure can leave DB state that invalidates cached new-image counts.
                _invalidate_new_images_after_scan(thread_db, scan_target)
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
                vireo_dir=vireo_dir,
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

        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

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
                vireo_dir=vireo_dir,
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
                    thread_db.update_photo_flag(r["photo_id"], "flagged",
                                                verify_workspace=False)
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
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

        def work(job):
            return run_classify_job(job, runner, db_path, active_ws, params, vireo_dir=vireo_dir)

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

    @app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
    def api_job_cancel(job_id):
        """Request cancellation of a running job.

        Returns 200 if the job was found running and marked for cancellation,
        404 if the job does not exist or is no longer running.
        """
        runner = app._job_runner
        if runner.cancel_job(job_id):
            return jsonify({"cancelled": True, "job_id": job_id})
        job = runner.get(job_id)
        if job is None:
            return json_error("job not found", 404)
        return json_error(f"job is not running (status={job['status']})", 404)

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
                        # Check if job is done (in case we missed the complete event).
                        # Include "cancelled" as a terminal state so cancelled jobs
                        # close the SSE stream instead of looping indefinitely.
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
        count = min(max(1, request.args.get("count", 100, type=int)), 1000)
        return jsonify(app._log_broadcaster.get_recent(count))

    @app.route("/api/jobs/history")
    def api_job_history():
        db = _get_db()
        limit = min(max(1, request.args.get("limit", 10, type=int)), 1000)
        return jsonify(app._job_runner.get_history(db, limit=limit))

    @app.route("/api/report-issue", methods=["POST"])
    def api_report_issue():
        """Collect diagnostics and optionally send to a configured report URL."""
        import platform
        import sys
        import urllib.request

        data = request.get_json(force=True, silent=True) or {}
        description = (data.get("description") or "").strip()
        if not description:
            return json_error("A description is required")

        # --- Version (same logic as api_version) ---
        try:
            from importlib.metadata import version as pkg_version
            vireo_version = pkg_version("vireo")
        except Exception:
            import tomllib
            try:
                with open(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"), "rb") as f:
                    vireo_version = tomllib.load(f)["project"]["version"]
            except Exception:
                vireo_version = "unknown"

        # --- App state ---
        db = None
        try:
            db = _get_db()
            ws = db.get_active_workspace()
            ws_name = ws["name"] if ws else "unknown"
            folder_count = db.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0]
            photo_count = db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
            # Predictions are global now; workspace scoping happens through
            # the detection -> photo -> workspace_folders join.
            pred_count = db.conn.execute(
                """SELECT COUNT(*) FROM predictions pr
                   JOIN detections d ON d.id = pr.detection_id
                   JOIN photos p ON p.id = d.photo_id
                   JOIN workspace_folders wf
                     ON wf.folder_id = p.folder_id AND wf.workspace_id = ?""",
                (db._ws_id(),)
            ).fetchone()[0]
        except Exception:
            ws_name = "unknown"
            folder_count = photo_count = pred_count = 0

        # --- Recent jobs ---
        try:
            recent_jobs = app._job_runner.get_history(db, limit=10)
        except Exception:
            recent_jobs = []

        # --- Config (sanitized) ---
        import config as cfg

        def _redact(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    if any(s in k.lower() for s in ("token", "key", "secret", "password")):
                        out[k] = "[REDACTED]"
                    else:
                        out[k] = _redact(v)
                return out
            if isinstance(obj, list):
                return [_redact(item) for item in obj]
            return obj

        sanitized_config = _redact(cfg.load())

        # --- Build the bundle ---
        from datetime import datetime

        bundle = {
            "description": description,
            "timestamp": datetime.now(UTC).isoformat(),
            "vireo_version": vireo_version,
            "system": {
                "platform": platform.platform(),
                "python": sys.version,
                "architecture": platform.machine(),
            },
            "logs": app._log_broadcaster.get_recent(200),
            "app_state": {
                "workspace": ws_name,
                "folders": folder_count,
                "photos": photo_count,
                "predictions": pred_count,
            },
            "recent_jobs": recent_jobs,
            "config": sanitized_config,
        }

        # --- Send or download ---
        # Fall back to plain cfg.load() if the DB is degraded (e.g. schema or
        # connection errors) so the download path still works when users are
        # reporting DB problems.
        try:
            effective = db.get_effective_config(cfg.load()) if db else cfg.load()
        except Exception:
            log.exception("Failed to load effective config for issue report")
            effective = cfg.load()
        report_url = effective.get("report_url", "")

        if report_url:
            try:
                payload = json.dumps(bundle).encode("utf-8")
                req = urllib.request.Request(
                    report_url,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                resp = urllib.request.urlopen(req, timeout=10)
                if 200 <= resp.status < 300:
                    return jsonify({"status": "sent"})
                else:
                    return jsonify({"status": "download", "diagnostics": bundle})
            except Exception:
                log.exception("Failed to send report to %s", report_url)
                return jsonify({"status": "download", "diagnostics": bundle})

        return jsonify({"status": "download", "diagnostics": bundle})

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
        import config as cfg
        ws = db._active_workspace_id
        min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        distance_threshold = request.args.get("threshold", 0.4, type=float)

        # Cluster within a single classifier model. A workspace whose
        # detections were classified under both BioCLIP-2 and BioCLIP-3
        # would otherwise emit one prediction row per model, double-count
        # the photo, and cluster vectors from incompatible model spaces.
        from models import get_active_model
        active_model = get_active_model()
        if not active_model or active_model.get("model_type", "bioclip") == "timm":
            return jsonify({
                "species": species_name,
                "clusters": [],
                "total_photos": 0,
            })
        classifier_model = active_model["name"]

        # Find all photos with this species prediction in the active
        # workspace. Predictions are global but membership in the
        # workspace is expressed through workspace_folders.
        #
        # Fingerprint filter: for the chosen classifier_model surface only
        # rows from the most recent labels_fingerprint. Without this, a
        # workspace that rotated label sets would cluster stale species
        # rows alongside current ones, distorting cluster membership /
        # counts / variant labels and duplicating the same photo embedding.
        rows = db.conn.execute(
            """SELECT d.photo_id, pe.embedding, p.filename, p.thumb_path,
                      pr.confidence, pr.taxonomy_order, pr.taxonomy_family
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               JOIN photo_embeddings pe
                 ON pe.photo_id = d.photo_id
                AND pe.model = pr.classifier_model
                AND pe.variant = ''
               WHERE pr.species = ?
                 AND pr.classifier_model = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )""",
            (ws, species_name, classifier_model, min_conf),
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
        """List all species with prediction counts, for the variant explorer.

        Scoped to photos in the active workspace via ``workspace_folders`` and
        filtered at read time by the workspace-effective
        ``detector_confidence`` threshold. Review status is sourced from the
        workspace-scoped ``prediction_review`` table; rejected predictions
        are excluded. Predictions are filtered to the most recent
        ``labels_fingerprint`` per ``(detection, classifier_model)`` so that
        stale species from old label sets do not contaminate counts after
        re-classification.
        """
        db = _get_db()
        import config as cfg
        ws = db._active_workspace_id
        min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        rows = db.conn.execute(
            """SELECT pr.species, COUNT(DISTINCT d.photo_id) as photo_count,
                      pr.taxonomy_order, pr.taxonomy_family, pr.taxonomy_genus,
                      pr.scientific_name
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos p ON p.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE d.detector_confidence >= ?
                 AND COALESCE(pr_rev.status, 'pending') != 'rejected'
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               GROUP BY pr.species
               ORDER BY photo_count DESC""",
            (ws, ws, min_conf),
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
                vireo_dir=os.path.dirname(app.config["THUMB_CACHE_DIR"]),
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
                "photos_missing_phash": result.get("photos_missing_phash", 0),
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
            from export import developed_folder_key

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

                # Determine output directory. The per-folder "developed/" default
                # is naturally disambiguated (one dir per source folder). The
                # globally configured dir is flat, so nest each photo under a
                # stable key derived from the source folder's path (not its row
                # id — SQLite reuses those after deletion, which would silently
                # cross-wire new folders onto stale developed files left on
                # disk by a previously-deleted folder).
                if output_dir:
                    out_dir = os.path.join(output_dir, developed_folder_key(folder_path))
                else:
                    out_dir = os.path.join(folder_path, "developed")
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

            result = {"developed": developed, "errors": errors, "total": total}
            if errors > 0:
                # Rollup rule: if any per-photo develop failed, the overall
                # job is failed (not completed). Stash the counts on the job
                # so _persist_job (jobs.py ~L197-210) merges them with the
                # primary error message, then raise so _run_job records
                # status="failed".
                #
                # Re-raise with an existing entry in job["errors"] so
                # _run_job's dedup guard (err_str not in job["errors"])
                # skips the append — otherwise a novel exception string
                # gets tacked on as a synthetic extra error, inflating
                # error_count by 1. Keep the nicer "N/M failed: <err>"
                # summary on job["_fatal_error"], which _persist_job
                # prefers over errors[0] when building the result row.
                job["result"] = result
                first_err = job["errors"][0]
                job["_fatal_error"] = (
                    f"{errors}/{total} develop operations failed: {first_err}"
                )
                raise RuntimeError(first_err)
            return result

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
                # Filter to photos with detections but no masks. Detection
                # threshold is resolved at read time from the workspace's
                # effective config (detections table is global now).
                photos = []
                for p in coll_photos:
                    if p["mask_path"]:
                        continue
                    dets = thread_db.get_detections(p["id"])
                    if dets:
                        det = dets[0]
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
                        variant=dinov2_variant,
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
        source_snapshot_id = body.get("source_snapshot_id")

        if not source and not sources and not collection_id and not source_snapshot_id:
            return json_error("source, sources, collection_id, or source_snapshot_id required")

        # Validate type before touching SQLite. Non-integer bodies (objects,
        # arrays, non-numeric strings, floats, bools) would otherwise reach
        # sqlite3 parameter binding and raise ProgrammingError, surfacing as
        # an opaque 500 instead of a clean 4xx.
        if source_snapshot_id is not None and (
            isinstance(source_snapshot_id, bool)
            or not isinstance(source_snapshot_id, int)
        ):
            return json_error("source_snapshot_id must be an integer")

        # Resolve the snapshot synchronously so clients get 404 at request
        # time instead of a 200 followed by an asynchronous job failure.
        if (
            source_snapshot_id is not None
            and _get_db().get_new_images_snapshot(source_snapshot_id) is None
        ):
            return json_error(
                f"source_snapshot_id {source_snapshot_id} not found",
                status=404,
            )

        # Validate source directories — skipped when a snapshot is present,
        # since run_pipeline_job overrides source/sources with the snapshot's
        # folders. Rejecting on stale placeholder paths would falsely 400 an
        # otherwise-valid snapshot-backed run.
        if source_snapshot_id is None:
            if sources:
                for s in sources:
                    if not os.path.isdir(s):
                        return json_error(f"source directory not found: {s}")
            elif source and not os.path.isdir(source):
                return json_error(f"source directory not found: {source}")

        destination = body.get("destination")
        # Copy-ingest ("destination") is incompatible with snapshot runs:
        # ingest would copy entire source folders, then snapshot filtering
        # would drop the destination-scanned photo ids, producing empty
        # downstream stages after an expensive copy. Fail fast.
        if destination and source_snapshot_id is not None:
            return json_error(
                "destination is not allowed when source_snapshot_id is set"
            )
        if destination and not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if destination and folder_template:
            from ingest import _is_unsafe_path
            if _is_unsafe_path(folder_template):
                return json_error("folder_template must be a relative path without '..' or backslashes")

        params = PipelineParams(
            collection_id=collection_id,
            source=source,
            sources=sources,
            source_snapshot_id=source_snapshot_id,
            destination=destination,
            file_types=body.get("file_types", "both"),
            folder_template=folder_template,
            skip_duplicates=body.get("skip_duplicates", True),
            labels_file=body.get("labels_file"),
            labels_files=body.get("labels_files"),
            model_id=body.get("model_id"),
            model_ids=body.get("model_ids"),
            reclassify=body.get("reclassify", False),
            skip_classify=body.get("skip_classify", False),
            download_taxonomy=body.get("download_taxonomy", True),
            skip_extract_masks=body.get("skip_extract_masks", False),
            skip_regroup=body.get("skip_regroup", False),
            preview_max_size=body.get("preview_max_size", 1920),
            exclude_paths=set(body.get("exclude_paths", [])) or None,
            exclude_photo_ids=set(body.get("exclude_photo_ids", [])) or None,
            recursive=body.get("recursive", True),
        )

        # Auto-skip classify stages if no model is available
        model_warning = None
        if not params.skip_classify:
            from models import get_active_model, get_models

            # Resolve the set of requested models. Prefer the explicit list,
            # fall back to the legacy single id, and finally to whatever is
            # marked active. Any requested id that isn't downloaded fails the
            # check, so the user sees the "no model available" warning instead
            # of a mid-run model_loader crash.
            requested_ids = list(params.model_ids or [])
            if not requested_ids and params.model_id:
                requested_ids = [params.model_id]

            all_models = None
            resolved_any = False
            if requested_ids:
                all_models = get_models()
                by_id = {m["id"]: m for m in all_models}
                resolved_any = all(
                    by_id.get(mid, {}).get("downloaded") for mid in requested_ids
                )
            else:
                resolved_any = get_active_model() is not None

            if not resolved_any:
                params.skip_classify = True
                params.skip_extract_masks = True
                params.skip_regroup = True
                model_warning = "No model available \u2014 classification was skipped. Download a model in Settings to enable species identification."

        # Save destination to recent list (best-effort — don't block pipeline)
        if destination:
            try:
                import config as cfg
                _cfg = cfg.load()
                ingest_cfg = dict(_cfg.get("ingest", {}))
                recents = list(ingest_cfg.get("recent_destinations", []))
                if destination in recents:
                    recents.remove(destination)
                recents.insert(0, destination)
                recents = recents[:5]
                ingest_cfg["recent_destinations"] = recents
                _cfg["ingest"] = ingest_cfg
                cfg.save(_cfg)
            except Exception:
                log.warning("Failed to save recent destination to config")

        runner = app._job_runner
        active_ws = _get_db()._active_workspace_id

        def work(job):
            return run_pipeline_job(
                job, runner, db_path, active_ws, params,
                thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
            )

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
        """Confirm species for all photos in an encounter or a single burst.

        Expects JSON: {"species": "Blue Jay", "photo_ids": [1, 2, 3],
                       "burst_index": <int|null>}

        Creates the species keyword, tags photos, and queues a sidecar add.
        If the encounter (or burst) was previously confirmed as a different
        species, also untags that species and queues a sidecar remove — or
        cancels the still-pending add if it hadn't synced yet — so the XMP
        doesn't accumulate stale species keywords.
        """
        db = _get_db()
        body = request.get_json(silent=True) or {}
        species = body.get("species", "").strip()
        photo_ids = body.get("photo_ids", [])
        burst_index = body.get("burst_index")

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

        # Look up the previous species (if any) from the pipeline cache before
        # mutating. We reuse the same cached dict to write the update below.
        from pipeline import load_results_raw, save_results_raw

        cache_dir = os.path.dirname(db_path)
        cached = load_results_raw(cache_dir, db._active_workspace_id)
        previous_species = None
        target_enc = None
        target_enc_idx = None
        if cached:
            photo_id_set = set(photo_ids)
            for enc_idx, enc in enumerate(cached.get("encounters", [])):
                enc_ids = set(enc.get("photo_ids", []))
                if not photo_id_set.issubset(enc_ids):
                    continue
                target_enc = enc
                target_enc_idx = enc_idx
                break

        # If this is a burst-scoped request, the burst must actually exist in
        # the cached encounter AND the submitted photo_ids must be a subset of
        # that burst's photos. Otherwise a stale client (e.g. one that still
        # holds a burst index from before a regrouping) could retag photos
        # that don't belong to this burst while the cache update below touches
        # the wrong override.
        if burst_index is not None:
            bursts = target_enc.get("bursts") if target_enc else None
            if not bursts or not (0 <= burst_index < len(bursts)):
                return json_error(
                    f"Unknown burst_index {burst_index} for submitted photos",
                )
            burst_photo_ids = set(bursts[burst_index].get("photo_ids", []))
            if not set(photo_ids).issubset(burst_photo_ids):
                return json_error(
                    f"photo_ids are not members of bursts[{burst_index}]",
                )

        if target_enc is not None:
            if burst_index is not None:
                ovr = target_enc["bursts"][burst_index].get("species_override")
                if ovr and ovr.get("species"):
                    previous_species = ovr["species"]
                else:
                    # No burst override yet — inherit the encounter's confirmed
                    # species, which is what those photos were actually tagged
                    # with.
                    previous_species = target_enc.get("confirmed_species")
            else:
                previous_species = target_enc.get("confirmed_species")

        ws_id = db._ws_id()

        old_kid = None
        is_replacement = (
            previous_species is not None
            and previous_species.strip().lower() != species.lower()
        )
        if is_replacement:
            # Match add_keyword's write path: species keywords live as root
            # keywords (parent_id IS NULL) with is_species=1. Looking up by
            # name alone could collide with a non-species homonym nested under
            # another keyword (schema allows UNIQUE(name, parent_id)).
            old_kid_row = db.conn.execute(
                """SELECT id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL
                     AND is_species = 1""",
                (previous_species,),
            ).fetchone()
            if old_kid_row:
                old_kid = old_kid_row["id"]

        # Run all mutations in a single transaction so that a mid-loop failure
        # (SQLite lock, disk error, etc.) can't leave half the photos retagged
        # while the other half still carry the old species.
        try:
            if is_replacement and old_kid is not None:
                for pid in photo_ids:
                    db.untag_photo(pid, old_kid, _commit=False)
                    _queue_keyword_remove(
                        pid, previous_species,
                        workspace_id=ws_id, _commit=False,
                    )

            kid = db.add_keyword(species, is_species=True, _commit=False)

            for pid in photo_ids:
                db.tag_photo(pid, kid, _commit=False)
                _queue_keyword_add(
                    pid, species, workspace_id=ws_id, _commit=False,
                )

            if is_replacement and old_kid is not None:
                items = [
                    {"photo_id": pid, "old_value": str(old_kid), "new_value": str(kid)}
                    for pid in photo_ids
                ]
                db.record_edit(
                    "species_replace",
                    f'Replaced species "{previous_species}" with "{species}" on {len(photo_ids)} photos',
                    str(kid),
                    items,
                    is_batch=len(photo_ids) > 1,
                    _commit=False,
                )
            else:
                items = [
                    {"photo_id": pid, "old_value": "", "new_value": str(kid)}
                    for pid in photo_ids
                ]
                db.record_edit(
                    "keyword_add",
                    f'Confirmed species "{species}" on {len(photo_ids)} photos',
                    str(kid),
                    items,
                    is_batch=len(photo_ids) > 1,
                    _commit=False,
                )
            db.conn.commit()
        except Exception:
            db.conn.rollback()
            raise
        # Prune oldest edit-history rows now that the transaction has landed.
        db._prune_edit_history()

        # Update pipeline cache. burst_index was validated above, so the
        # branch here is unambiguous: burst-scoped requests only touch the
        # burst override, encounter-scoped requests only touch the encounter.
        if cached and target_enc is not None:
            if burst_index is not None:
                target_enc["bursts"][burst_index]["species_override"] = {
                    "species": species,
                    "confirmed": True,
                }
                # Auto-detach if burst's confirmed species differs from its
                # encounter — splits it out and merges into an adjacent
                # encounter of the same confirmed species when one exists.
                enc_species = target_enc.get("confirmed_species") or (
                    target_enc["species"][0] if target_enc.get("species") else None
                )
                if (
                    enc_species is not None
                    and enc_species != species
                    and len(target_enc["bursts"]) > 1
                ):
                    _auto_detach_burst_for_species(
                        cached, target_enc_idx, burst_index, species
                    )
            else:
                target_enc["species_confirmed"] = True
                target_enc["confirmed_species"] = species
            save_results_raw(cached, cache_dir, db._active_workspace_id)

        replaced = (
            previous_species
            if previous_species and previous_species.strip().lower() != species.lower()
            else None
        )
        response = {
            "ok": True,
            "species": species,
            "keyword_id": kid,
            "photo_count": len(photo_ids),
            "previous_species": replaced,
        }
        if cached:
            response["encounters"] = cached.get("encounters", [])
            response["summary"] = cached.get("summary", {})
        return jsonify(response)

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
        # Get primary detection from global detections table (threshold
        # resolved from workspace-effective config inside get_detections).
        dets = db.get_detections(photo_id)
        if dets:
            det = dets[0]
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
            load_results_raw,
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

        # Carry the miss-recomputation marker through so the review UI's
        # "Review misses" shortcut stays visible after a threshold
        # tweak. reflow/regroup-live do not recompute misses themselves.
        cache_dir = os.path.dirname(db_path)
        existing = load_results_raw(cache_dir, db._active_workspace_id)
        if existing and existing.get("miss_computed_at"):
            results["miss_computed_at"] = existing["miss_computed_at"]

        # Save updated results
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
            load_results_raw,
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

        # Carry the miss-recomputation marker through so the review UI's
        # "Review misses" shortcut stays visible after a threshold
        # tweak. regroup-live does not rerun the miss stage itself.
        cache_dir = os.path.dirname(db_path)
        existing = load_results_raw(cache_dir, db._active_workspace_id)
        if existing and existing.get("miss_computed_at"):
            results["miss_computed_at"] = existing["miss_computed_at"]

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

        # Share the schema-driven settings write lock so a concurrent schema
        # autosave can't read this same overrides snapshot and overwrite the
        # pipeline change with stale data.
        with _settings_write_lock:
            ws = db.get_workspace(db._active_workspace_id)
            current_overrides = {}
            if ws and ws["config_overrides"]:
                try:
                    current_overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
                except (json.JSONDecodeError, TypeError):
                    pass
            if not isinstance(current_overrides, dict):
                current_overrides = {}

            pipeline_section = current_overrides.get("pipeline", {})
            if not isinstance(pipeline_section, dict):
                pipeline_section = {}
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

        # Pre-validate all photo IDs against workspace before any mutations
        for pid in keepers + rejects:
            if not db._photo_in_workspace(pid):
                return json_error(f"Photo {pid} is not in the active workspace", 403)

        # Capture old flags before mutation
        old_flags = {}
        for pid in keepers + rejects:
            old = db.get_photo(pid)
            if old:
                old_flags[pid] = old["flag"] or "none"

        try:
            for pid in keepers:
                db.update_photo_flag(pid, "flagged")
            for pid in rejects:
                db.update_photo_flag(pid, "rejected")
        except ValueError as e:
            return json_error(str(e), 403)

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

        limit = min(max(1, request.args.get("limit", 50, type=int)), 1000)
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
        emb_pairs = db.get_photos_with_embedding(model_name)
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
        limit = min(max(1, request.args.get("limit", 20, type=int)), 1000)

        # Compare against the active classifier's embedding. The per-model
        # cache means a photo classified under BioCLIP-2 cannot be compared
        # to one classified under BioCLIP-3 — that mixing is exactly what
        # Phase 1 of the storage philosophy refactor stops.
        from models import get_active_model
        active_model = get_active_model()
        if not active_model:
            return json_error("No active classifier configured")
        model_name = active_model["name"]
        if active_model.get("model_type", "bioclip") == "timm":
            return json_error("Active classifier does not produce embeddings")

        source_blob = db.get_photo_embedding(photo_id, model_name)
        if not source_blob:
            return json_error(
                f"No {model_name} embedding for this photo — "
                "run classification first"
            )
        source_emb = np.frombuffer(source_blob, dtype=np.float32)

        # Load all workspace embeddings for the same model, then drop the
        # source photo before stacking.
        rows = [
            (pid, blob)
            for pid, blob in db.get_photos_with_embedding(model_name)
            if pid != photo_id
        ]

        if not rows:
            return jsonify({"similar": [], "total_compared": 0})

        # Compute cosine similarities (embeddings are already normalized)
        photo_ids = []
        embeddings = []
        for pid, blob in rows:
            photo_ids.append(pid)
            embeddings.append(np.frombuffer(blob, dtype=np.float32))

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

        # Get detections for this photo — threshold resolved at read time
        # from the workspace-effective config.
        dets = db.get_detections(photo_id)
        result["detections"] = [dict(d) for d in dets]

        # Primary detection = highest-confidence above threshold.
        if dets:
            primary = dets[0]
            result["detection_box"] = {
                "x": primary["box_x"], "y": primary["box_y"],
                "w": primary["box_w"], "h": primary["box_h"],
            }
            result["detection_conf"] = primary["detector_confidence"]

        # Get predictions for this photo (through detections JOIN).  Per-
        # workspace review state (status, group_id, individual, vote counts)
        # is left-joined from prediction_review; absent rows are 'pending'.
        #
        # Apply the same workspace-effective detector_confidence floor used
        # by `db.get_detections` above so result["predictions"] stays in
        # sync with result["detections"]. Otherwise raising the threshold
        # leaves stale species rows for detections the UI is meant to hide.
        # Also pin to the most recent labels_fingerprint per
        # (detection, classifier_model) so a workspace that rotated label
        # sets doesn't see a debug payload mixing stale and current labels.
        import config as cfg
        ws = db._active_workspace_id
        min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
        preds = db.conn.execute(
            """SELECT pr.species, pr.confidence, pr.classifier_model AS model,
                      pr.category,
                      COALESCE(pr_rev.status, 'pending') AS status,
                      pr_rev.individual AS individual,
                      pr_rev.group_id AS group_id,
                      pr_rev.vote_count AS vote_count,
                      pr_rev.total_votes AS total_votes,
                      d.box_x, d.box_y, d.box_w, d.box_h, d.detector_confidence
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               WHERE d.photo_id = ?
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                    SELECT pr2.labels_fingerprint FROM predictions pr2
                    WHERE pr2.detection_id = pr.detection_id
                      AND pr2.classifier_model = pr.classifier_model
                    ORDER BY pr2.created_at DESC, pr2.id DESC
                    LIMIT 1
                 )
               ORDER BY pr.confidence DESC""",
            (ws, photo_id, min_conf),
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
        photo = db.get_photo(photo_id)
        if not photo:
            return "Not found", 404

        # Try working copy first, fall back to original
        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        image_path = None
        if photo["working_copy_path"]:
            wc = os.path.join(vireo_dir, photo["working_copy_path"])
            if os.path.exists(wc):
                image_path = wc
        if image_path is None:
            folder = db.conn.execute(
                "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
            ).fetchone()
            if not folder:
                return "Not found", 404
            image_path = os.path.join(folder["path"], photo["filename"])

        img = load_image(image_path, max_size=None)
        if img is None:
            return "Could not load image", 500

        # Get primary detection box — global detections table, threshold
        # resolved from workspace-effective config in get_detections.
        dets = db.get_detections(photo_id)
        det_box = None
        if dets:
            det_row = dets[0]
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

    def allowed_preview_sizes():
        """Allowlist for /photos/<id>/preview?size=N.

        Includes the fixed tier plus the user-configured preview_max_size
        so /full can delegate here. Reads workspace-effective config so
        a per-workspace preview_max_size override is honored.
        """
        import config as cfg
        effective = _get_db().get_effective_config(cfg.load())
        fixed = {1920, 2560, 3840}
        pm = effective.get("preview_max_size") or 1920
        if pm == 0:
            return fixed  # 0 = "full" — handled by /original path
        return fixed | {int(pm)}


    def _serve_preview(photo_id, size):
        """Serve a preview at the given size, using the preview_cache LRU.

        This is the single code path behind both /photos/<id>/preview and
        /photos/<id>/full. Callers have already validated size.
        """
        import io

        import config as cfg
        from flask import send_file

        # Confirm the photo still exists before any cache return so that a
        # deleted photo can't be served from a stale per-size cache (and so
        # SQLite id reuse can't surface the wrong image).
        db = _get_db()
        photo = db.get_photo(photo_id)
        if not photo:
            return "Not found", 404

        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(vireo_dir, "previews")
        cache_path = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")

        # Reject corrupt zero-byte cache files (prior write interrupted).
        # Treat them as a miss so the regeneration path below produces a
        # real preview.
        if os.path.exists(cache_path) and os.path.getsize(cache_path) == 0:
            try:
                os.remove(cache_path)
            except OSError:
                pass
            db.preview_cache_delete(photo_id, size)  # no-op if no row

        # Cache hit (tracked): touch and serve. The touch is best-effort
        # bookkeeping — under concurrent traffic SQLite can raise
        # OperationalError: database is locked, but that shouldn't turn a
        # valid cache hit into a 500 when the JPEG is right there on disk.
        if db.preview_cache_get(photo_id, size) and os.path.exists(cache_path):
            try:
                db.preview_cache_touch(photo_id, size)
            except Exception:
                pass
            return send_file(cache_path, mimetype="image/jpeg")

        # Cache hit (on-disk but untracked): lazy adoption.
        # preview_cache_insert uses time.time() for last_access_at, so the
        # adopted entry is ranked as freshly-accessed in the LRU in a single
        # commit (instead of insert-with-mtime-then-touch-to-now).
        # Read bytes into memory before evicting: eviction may delete the
        # file we just adopted (e.g. preview_cache_max_mb=0), but we can
        # still serve the response from memory — mirrors the miss path.
        if os.path.exists(cache_path):
            with open(cache_path, "rb") as f:
                data = f.read()
            try:
                db.preview_cache_insert(photo_id, size, len(data))
                evict_preview_cache_if_over_quota(db, vireo_dir)
            except Exception:
                pass
            return Response(data, mimetype="image/jpeg")

        # Cache miss: generate, insert, evict-if-over-quota, serve.
        from image_loader import get_canonical_image_path, load_image

        folder_row = db.conn.execute(
            "SELECT id, path FROM folders WHERE id=?", (photo["folder_id"],)
        ).fetchone()
        if not folder_row:
            return "Not found", 404
        folders = {folder_row["id"]: folder_row["path"]}

        canonical = get_canonical_image_path(photo, vireo_dir, folders)
        img = load_image(canonical, max_size=size)
        if img is None:
            return "Could not load image", 500

        preview_quality = cfg.load().get("preview_quality", 90)

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=preview_quality)
        data = buf.getvalue()

        # Persist to disk and track in the LRU. Eviction may delete the file
        # we just wrote (e.g. when preview_cache_max_mb is 0), but that's fine:
        # we serve the bytes from memory below so disk state doesn't matter.
        # Catch broadly: OSError for disk failures, sqlite3 errors for DB
        # lock / FK violations (photo deleted between lookup and insert).
        # The preview bytes are ready in memory — never fail the request
        # over bookkeeping.
        try:
            os.makedirs(preview_dir, exist_ok=True)
            with open(cache_path, "wb") as f:
                f.write(data)
            db.preview_cache_insert(photo_id, size, len(data))
            evict_preview_cache_if_over_quota(db, vireo_dir)
        except Exception as e:
            log.warning(
                "Failed to persist preview cache %s: %s", cache_path, e,
            )

        return Response(data, mimetype="image/jpeg")

    @app.route("/photos/<int:photo_id>/full")
    def serve_full_photo(photo_id):
        """Serve a display-sized preview (alias for /preview at preview_max_size).

        Reads workspace-effective config so a per-workspace preview_max_size
        override is honored. preview_max_size == 0 historically meant "full"
        — we route to /original rather than generate a preview. Using a
        separate read/fallback (instead of `cfg.get(...) or 1920`) keeps
        the 0 sentinel reachable.
        """
        import config as cfg
        from flask import redirect

        effective = _get_db().get_effective_config(cfg.load())
        pm = effective.get("preview_max_size")
        if pm == 0:
            return redirect(f"/photos/{photo_id}/original")
        return _serve_preview(photo_id, int(pm or 1920))

    @app.route("/photos/<int:photo_id>/preview")
    def serve_photo_preview(photo_id):
        """Serve a JPEG preview at a chosen max-size.

        Cache is LRU-tracked in the preview_cache table; on-disk files that
        predate this scheme are adopted lazily on first access.

        Query params:
          size: int — max dimension (longest side). Must be in
                allowed_preview_sizes() to avoid unbounded cache growth.
        """
        from flask import request

        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        if size not in allowed_preview_sizes():
            return "Unsupported size", 400
        return _serve_preview(photo_id, size)

    @app.route("/photos/<int:photo_id>/original")
    def serve_original_photo(photo_id):
        """Serve full-resolution image for 1:1 zoom."""
        import config as cfg
        from flask import send_file

        db = _get_db()
        photo = db.get_photo(photo_id)
        if not photo:
            return "Not found", 404

        vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])

        # Decide whether to trust the working copy as the full-res asset
        # by reading its actual on-disk dimensions, NOT the current
        # ``working_copy_max_size`` config — the cap may have changed
        # since the wc was generated, leaving stale capped wcs that
        # config-based logic would misclassify as full-res.
        #
        # PIL.Image.open is lazy: it reads the JPEG SOF marker for
        # ``.size`` without decoding pixels (sub-millisecond), so this
        # is safe to do per request even during burst-review zoom. The
        # expensive path we must avoid is the RAW re-extract below
        # (5–7s per photo), not the header read.
        if photo["working_copy_path"]:
            wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
            if os.path.exists(wc_path):
                from PIL import Image as _PILImage
                try:
                    with _PILImage.open(wc_path) as _wc_img:
                        wc_w, wc_h = _wc_img.size
                except Exception:
                    wc_w = wc_h = 0
                orig_w = photo["width"] or 0
                orig_h = photo["height"] or 0
                # Trust the wc when it meets/exceeds the believed
                # original dims, or when those dims are unknown (no
                # basis to declare the wc stale and a speculative RAW
                # re-extract would just thrash the disk).
                if wc_w and wc_h and (
                    (wc_w >= orig_w and wc_h >= orig_h)
                    or not (orig_w and orig_h)
                ):
                    return send_file(wc_path, mimetype="image/jpeg")
                # The wc is smaller than the believed original. For RAW
                # sources this often means rawpy.postprocess() failed
                # and we fell back to the embedded JPEG, which can be a
                # few pixels shy of the full sensor area (e.g. Nikon
                # NEFs report 8280×5520 but the embedded JPEG is
                # 8256×5504). Re-extracting would yield the same
                # fallback image, just slower — so trust the wc when
                # it is within 1% of the believed dims. This tolerance
                # is RAW-only: for JPEG/PNG/etc., the wc being smaller
                # means the cap downsized it, and re-extracting WILL
                # produce more pixels.
                from image_loader import RAW_EXTENSIONS
                ext = os.path.splitext(photo["filename"])[1].lower()
                if ext in RAW_EXTENSIONS and wc_w and wc_h:
                    wc_long = max(wc_w, wc_h)
                    orig_long = max(orig_w, orig_h)
                    if orig_long and wc_long >= orig_long * 0.99:
                        return send_file(wc_path, mimetype="image/jpeg")
                # Fall through to on-demand re-extract.

        # Resolve original file path
        folder = db.conn.execute(
            "SELECT path FROM folders WHERE id=?", (photo["folder_id"],)
        ).fetchone()
        if not folder:
            return "Not found", 404
        image_path = os.path.join(folder["path"], photo["filename"])

        # For browser-native formats without a working copy, serve directly
        ext = os.path.splitext(photo["filename"])[1].lower()
        if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp") and not photo["working_copy_path"] and os.path.exists(image_path):
            return send_file(image_path)

        # Extract full-res working copy (on-demand upgrade)
        from image_loader import extract_working_copy, load_image
        wc_rel = f"working/{photo_id}.jpg"
        wc_abs = os.path.join(vireo_dir, wc_rel)
        quality = cfg.load().get("working_copy_quality", 92)

        # Prefer companion JPEG as extraction source — avoids slow RAW decode.
        # Only use it when we can confirm it is full-resolution: its pixel
        # dimensions must be at least as large as the stored original
        # dimensions.  If original dimensions are unknown (None) we cannot
        # make that guarantee, so fall back to decoding from the RAW.
        source_for_extraction = image_path
        if photo["companion_path"]:
            companion_abs = os.path.join(folder["path"], photo["companion_path"])
            if os.path.exists(companion_abs):
                orig_w = photo["width"]
                orig_h = photo["height"]
                if orig_w and orig_h:
                    from PIL import Image as _PILImage
                    try:
                        with _PILImage.open(companion_abs) as _cimg:
                            c_w, c_h = _cimg.size
                        if c_w >= orig_w and c_h >= orig_h:
                            source_for_extraction = companion_abs
                    except Exception:
                        pass  # unreadable companion — fall back to RAW
                # If original dims are unknown, skip companion (can't verify resolution)

        if extract_working_copy(source_for_extraction, wc_abs, max_size=0, quality=quality):
            # Update DB so future requests are fast; also backfill
            # dimensions if missing so the full-res shortcut works next time
            from PIL import Image as _PILImage
            with _PILImage.open(wc_abs) as upgraded:
                uw, uh = upgraded.size
            updates = ["working_copy_path=?"]
            params = [wc_rel]
            if not photo["width"] or not photo["height"]:
                updates.extend(["width=?", "height=?"])
                params.extend([uw, uh])
            params.append(photo_id)
            db.conn.execute(
                f"UPDATE photos SET {', '.join(updates)} WHERE id=?",
                params,
            )
            db.conn.commit()
            return send_file(wc_abs, mimetype="image/jpeg")

        # Fallback: serve via load_image
        img = load_image(image_path, max_size=None)
        if img is None:
            return "Could not load image", 500
        originals_dir = os.path.join(vireo_dir, "originals")
        cache_path = os.path.join(originals_dir, f"{photo_id}.jpg")
        os.makedirs(originals_dir, exist_ok=True)
        img.save(cache_path, format="JPEG", quality=quality)
        return send_file(cache_path, mimetype="image/jpeg")

    # -- Logs page --

    @app.route("/logs")
    def logs_page():
        _auto_open_tab("logs")
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

    # --- /api/v1/* aliases over the stable subset of /api/* ---
    # These are the endpoints advertised to external callers in docs/headless-api.md.
    # Keep this list tight — expanding it locks the surface.
    _V1_ALIASES = [
        # (v1 path, existing endpoint name, methods)
        ("/api/v1/photos", "api_photos", ["GET"]),
        ("/api/v1/photos/<int:photo_id>", "api_photo_detail", ["GET"]),
        ("/api/v1/collections", "api_collections", ["GET"]),
        ("/api/v1/collections/<int:collection_id>/photos",
         "api_collection_photos", ["GET"]),
        ("/api/v1/workspaces", "api_get_workspaces", ["GET"]),
        ("/api/v1/workspaces/<int:ws_id>/activate",
         "api_activate_workspace", ["POST"]),
        ("/api/v1/keywords", "api_keywords", ["GET"]),
    ]

    for v1_path, endpoint_name, methods in _V1_ALIASES:
        view = app.view_functions.get(endpoint_name)
        if view is None:
            raise RuntimeError(
                f"Cannot alias {v1_path}: endpoint '{endpoint_name}' not registered"
            )
        app.add_url_rule(
            v1_path,
            endpoint=f"v1_{endpoint_name}",
            view_func=view,
            methods=methods,
        )

    return app


def main():
    _setup_file_logging()

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
        "--headless",
        action="store_true",
        help="Run without opening a browser; write runtime.json and enable "
             "the /api/v1 API. Use this when invoking the sidecar directly "
             "from scripts or agents.",
    )
    parser.add_argument(
        "--load-taxonomy",
        action="store_true",
        help="Download and import the iNaturalist taxonomy, then exit",
    )
    args = parser.parse_args()

    if args.headless:
        args.no_browser = True

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

    from runtime import (
        acquire_single_instance,
        delete_runtime_json,
        generate_token,
        release_single_instance,
        write_runtime_json,
    )

    # Atomically reserve the single-instance slot BEFORE any heavy
    # initialization. Reserving up-front (rather than writing runtime.json
    # at the end of startup) closes the race where two near-simultaneous
    # launches both see an empty slot and both start serving.
    try:
        status, info = acquire_single_instance(pid=os.getpid())
    except OSError as e:
        # Filesystem fault opening the lock file (unreadable ~/.vireo,
        # permission denied, etc). Surface the real cause rather than
        # misreporting as already_running — the two need different
        # remediation.
        import sys as _sys
        _sys.stderr.write(json.dumps({
            "error": "startup_failed",
            "reason": str(e),
        }) + "\n")
        raise SystemExit(2) from e
    if status == "conflict":
        import sys as _sys
        _sys.stderr.write(json.dumps({
            "error": "already_running",
            "port": info.get("port"),
            "pid": info.get("pid"),
        }) + "\n")
        raise SystemExit(1)

    # Register cleanup immediately after acquiring the slot so a crash
    # during initialization still releases the reservation lock and any
    # runtime.json we may have written.
    import atexit
    import signal as _signal

    def _cleanup_runtime_state():
        delete_runtime_json()
        release_single_instance()

    atexit.register(_cleanup_runtime_state)
    _signal.signal(_signal.SIGTERM, lambda *_: (_cleanup_runtime_state(), os._exit(0)))

    api_token = generate_token()
    mode = "headless" if args.headless else "gui"

    app = create_app(
        db_path=args.db, thumb_cache_dir=args.thumb_dir, api_token=api_token,
    )

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

    # Look up the running version using the same fallback chain as
    # /api/version: package metadata, then pyproject.toml, then "0.0.0".
    # In source/dev runs where importlib.metadata is missing but
    # pyproject.toml is present, runtime.json must agree with
    # /api/v1/version — external callers use it to make compatibility
    # decisions and a bare "0.0.0" would mislead them.
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

    # Finalize runtime.json, replacing the reservation marker with the full
    # payload now that the port and token are known. Cleanup handlers were
    # registered immediately after `acquire_single_instance` above.
    write_runtime_json(
        port=port, pid=os.getpid(), version=ver, db_path=args.db,
        token=api_token, mode=mode,
    )

    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    # In a PyInstaller bundle, multiprocessing workers re-execute this binary.
    # Without freeze_support, the child runs main() — argparse rejects the
    # `--multiprocessing-fork ...` argv (or the `-c` bootstrap), the child
    # exits, and the parent gets EOFError on the handshake socket. The
    # PyInstaller runtime hook installs a freeze_support that intercepts
    # those argv shapes and runs the worker bootstrap instead, but only
    # when we actually call it.
    import multiprocessing
    multiprocessing.freeze_support()
    main()
