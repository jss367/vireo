"""System endpoints: health and version, shutdown, diagnostics, OS integration.

The health and version probes are dependency-free so the desktop shell can
poll them during startup. The rest are the app-wide admin and status routes:
``/api/shutdown`` (and the token-gated ``/api/v1/shutdown``), first-launch
``/api/setup/complete``, ``/api/system/*`` runtime info and the ExifTool
install and status checks, ``/api/scan/status`` dashboard counts, the server
log stream and issue reporting, mounted-volume listing, the recent
destinations history, and revealing a photo or folder in the OS file manager.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import queue
import subprocess
import sys
import threading
from datetime import UTC
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path

import config as cfg
from config import read_raw_config_file, settings_write_lock
from flask import Blueprint, Response, jsonify, request
from proc import no_window_kwargs
from runtime_warnings import runtime_execution_info

log = logging.getLogger(__name__)

# Serializes Windows SetErrorMode calls. SetErrorMode is process-wide, so
# concurrent /api/volumes requests could otherwise interleave save/restore
# and leave the process in the wrong mode mid-probe.
_WIN_ERROR_MODE_LOCK = threading.Lock()


def _application_version():
    try:
        return package_version("vireo")
    except PackageNotFoundError:
        import tomllib

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        try:
            with pyproject.open("rb") as handle:
                return tomllib.load(handle)["project"]["version"]
        except (OSError, KeyError, TypeError, ValueError):
            return "0.0.0"


def create_system_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    get_log_broadcaster,
):
    """Build the system blueprint.

    ``config`` is the Flask app's config mapping (``THUMB_CACHE_DIR``), read
    when a request runs rather than when the app is built; ``db_path`` is the
    catalog file whose size ``/api/scan/status`` reports. ``get_runner`` and
    ``get_log_broadcaster`` return the app's ``JobRunner`` and
    ``LogBroadcaster`` (``app._job_runner`` / ``app._log_broadcaster``),
    looked up per request. ``/api/setup/complete`` and
    ``/api/recent-destinations`` read-modify-write the settings file through
    ``config.read_raw_config_file`` under ``config.settings_write_lock``.

    The shutdown endpoints are exempt from ``create_app``'s workspace mutation
    reservation by their blueprint-qualified names (``system.api_shutdown``,
    ``system.api_v1_shutdown``).
    """
    blueprint = Blueprint("system", __name__)

    @blueprint.get("/api/health")
    def health():
        return jsonify({"status": "ok"})

    @blueprint.get("/api/v1/health")
    def stable_health():
        # The marker lets the native single-instance probe distinguish Vireo
        # from an unrelated service that happens to reuse a stale port.
        from runtime import SERVICE_MARKER

        return jsonify({"service": SERVICE_MARKER, "status": "ok"})

    @blueprint.get("/api/version")
    @blueprint.get("/api/v1/version")
    def version():
        return jsonify({"version": _application_version()})

    @blueprint.route("/api/v1/shutdown", methods=["POST"])
    def api_v1_shutdown():
        import signal
        import threading

        def _shutdown():
            os.kill(os.getpid(), signal.SIGTERM)

        threading.Timer(0.5, _shutdown).start()
        return jsonify({"status": "shutting_down"})

    @blueprint.route("/api/shutdown", methods=["POST"])
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

    @blueprint.route("/api/setup/complete", methods=["POST"])
    def api_setup_complete():
        """Mark first-launch setup as done (called after download or skip)."""
        # Lock + raw read-modify-write so a concurrent settings PATCH isn't
        # reverted and we don't pin every DEFAULTS value into the user's
        # file (see api_pipeline_save_grouping_defaults).
        with settings_write_lock:
            raw = read_raw_config_file()
            raw["setup_complete"] = True
            cfg.save(raw)
        return jsonify({"ok": True})

    @blueprint.route("/api/files/reveal", methods=["POST"])
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

        db = get_db()
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
                proc = subprocess.run(
                    ["open", "-R", "--", path],
                    timeout=5,
                    check=False,
                    capture_output=True,
                    text=True,
                    **no_window_kwargs(),
                )
            elif sys.platform.startswith("win"):
                # explorer.exe's exit status is not a reliable success signal
                # (see the returncode-check gate below), so a stale catalog
                # entry whose path no longer exists would otherwise silently
                # return ok=True and produce a bogus "Revealed in File
                # Explorer" toast. Preflight the target path so those cases
                # surface as failures instead.
                if is_folder:
                    if not os.path.isdir(path):
                        return jsonify({"ok": False, "reason": "folder not found"})
                    # Open the folder itself so the user sees its contents.
                    proc = subprocess.run(
                        ["explorer", path],
                        timeout=5,
                        check=False,
                        capture_output=True,
                        text=True,
                        **no_window_kwargs(),
                    )
                else:
                    if not os.path.isfile(path):
                        return jsonify({"ok": False, "reason": "photo file not found"})
                    proc = subprocess.run(
                        ["explorer", f"/select,{path}"],
                        timeout=5,
                        check=False,
                        capture_output=True,
                        text=True,
                        **no_window_kwargs(),
                    )
            else:
                # xdg-open on a file has inconsistent behavior across desktops
                # (some open the image viewer, not the file manager), so for
                # photo reveals we open the parent directory instead. Passing
                # a directory to xdg-open opens the folder in the file manager,
                # which is exactly what we want for folder reveals.
                #
                # Because photo reveals target the parent directory rather than
                # the file itself, xdg-open will happily succeed for a stale
                # catalog entry whose folder still exists but whose photo has
                # been deleted or moved — the user gets a "revealed" toast even
                # though nothing is selected. Verify the photo file exists first
                # so those cases surface as failures instead of false successes.
                if not is_folder and not os.path.isfile(path):
                    return jsonify({"ok": False, "reason": "photo file not found"})
                target = path if is_folder else (os.path.dirname(path) or path)
                # xdg-open doesn't honor `--`; abspath guarantees a leading `/`
                # so a crafted leading-dash path can't be parsed as a flag.
                target = os.path.abspath(target)
                proc = subprocess.run(
                    ["xdg-open", target],
                    timeout=5,
                    check=False,
                    capture_output=True,
                    text=True,
                    **no_window_kwargs(),
                )
            # explorer.exe's exit status is not a reliable success signal:
            # `explorer /select,<path>` (and `explorer <folder>`) routinely
            # return 1 even when File Explorer opens correctly, with nothing
            # useful on stdout/stderr. Skipping the returncode check on Windows
            # avoids reporting "reveal failed" for reveals that actually worked.
            if not sys.platform.startswith("win") and proc.returncode != 0:
                reason = (proc.stderr or proc.stdout or "").strip()
                if not reason:
                    reason = f"reveal command exited {proc.returncode}"
                return jsonify({"ok": False, "reason": reason})
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
            return jsonify({"ok": False, "reason": str(exc)})
        return jsonify({"ok": True})

    @blueprint.route("/api/system/install-exiftool", methods=["POST"])
    def api_install_exiftool():
        """Install ExifTool where Vireo can safely automate installation."""
        import subprocess

        from metadata import clear_exiftool_cache, exiftool_status, find_homebrew

        # Reprobe: a broken bundled copy may have cached True previously,
        # or the caller may be retrying after a failed install.
        clear_exiftool_cache()
        status = exiftool_status()
        if status["available"]:
            return jsonify({"success": True, "message": "exiftool is already installed"})

        if sys.platform.startswith("win"):
            return jsonify({
                "success": False,
                "error": (
                    "The Windows desktop build includes ExifTool. Repair or reinstall "
                    "Vireo if the bundled copy is unavailable."
                ),
            })
        if sys.platform != "darwin":
            return jsonify({
                "success": False,
                "error": "Install ExifTool with your Linux package manager, then restart Vireo.",
            })

        brew = find_homebrew()
        if not brew:
            return jsonify({
                "success": False,
                "error": "Homebrew is not installed. Install it from https://brew.sh, then run: brew install exiftool",
            })

        try:
            result = subprocess.run(
                [brew, "install", "exiftool"],
                capture_output=True, text=True, timeout=300,
                **no_window_kwargs(),
            )
            if result.returncode == 0:
                # A GUI-launched app may not inherit Homebrew's bin directory
                # on PATH. Re-resolve and probe the installed tool before
                # claiming Repair succeeded; ``find_exiftool`` also checks
                # Homebrew's standard off-PATH locations on macOS.
                clear_exiftool_cache()
                installed = exiftool_status()
                if installed["available"]:
                    return jsonify({
                        "success": True,
                        "message": "exiftool installed successfully",
                        "exiftool": installed,
                    })
                return jsonify({
                    "success": False,
                    "error": (
                        "Homebrew finished, but Vireo could not run ExifTool. "
                        "Run 'brew install exiftool' in Terminal, then retry."
                    ),
                    "exiftool": installed,
                })
            else:
                return jsonify({
                    "success": False,
                    "error": f"brew install failed: {result.stderr[:500]}",
                })
        except subprocess.TimeoutExpired:
            return jsonify({"success": False, "error": "Installation timed out after 5 minutes"})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    @blueprint.route("/api/recent-destinations", methods=["POST"])
    def api_recent_destinations_add():
        """Remember a destination selected in a folder picker."""
        import config as cfg

        body = request.get_json(silent=True) or {}
        destination = body.get("path")
        if not isinstance(destination, str) or not destination:
            return json_error("path is required", status=400)
        if not os.path.isabs(destination):
            return json_error("path must be absolute", status=400)

        # Use a raw read-modify-write so recording this convenience history
        # neither pins DEFAULTS into config.json nor races settings autosaves.
        with settings_write_lock:
            raw = read_raw_config_file()
            ingest = raw.get("ingest")
            ingest = dict(ingest) if isinstance(ingest, dict) else {}
            existing = ingest.get("recent_destinations")
            existing = existing if isinstance(existing, list) else []
            recents = [
                item for item in existing
                if isinstance(item, str) and item and item != destination
            ]
            recents.insert(0, destination)
            recents = recents[:5]
            ingest["recent_destinations"] = recents
            raw["ingest"] = ingest
            cfg.save(raw)

        return jsonify({"ok": True, "recent_destinations": recents})

    @blueprint.route("/api/exiftool/status")
    def api_exiftool_status():
        """Report whether the exiftool binary is installed.

        exiftool is the metadata backbone for every scan (capture date, GPS,
        camera/lens, dimensions). When it's absent scans still complete but
        produce no metadata, so the welcome flow and Settings surface this
        check up-front rather than letting the failure stay silent in the log.
        """
        from metadata import exiftool_status
        return jsonify(exiftool_status())

    @blueprint.route("/api/volumes", methods=["GET"])
    def api_volumes():
        """List mounted volumes (macOS/Windows/Linux) to help find SD cards."""
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

        def _scan_windows_drives() -> None:
            """Enumerate mounted drive letters via the Win32 API."""
            import ctypes
            import string

            kernel32 = ctypes.windll.kernel32
            # Suppress the "no disk in drive" hardware error dialog that can
            # otherwise pop up when probing not-ready removable drives.
            # SetErrorMode is process-wide, so the save/probe/restore must
            # be serialized — without the lock, concurrent /api/volumes
            # requests can interleave and leave the process in the wrong
            # mode mid-probe.
            SEM_FAILCRITICALERRORS = 0x0001
            with _WIN_ERROR_MODE_LOCK:
                old_mode = kernel32.SetErrorMode(SEM_FAILCRITICALERRORS)
                try:
                    bitmask = kernel32.GetLogicalDrives()
                    for i, letter in enumerate(string.ascii_uppercase):
                        if not (bitmask >> i) & 1:
                            continue
                        root = f"{letter}:\\"
                        # Skip drives with no media inserted (empty card
                        # readers, optical drives) — only ready drives are
                        # real volumes.
                        if not os.path.isdir(root):
                            continue
                        label = None
                        try:
                            buf = ctypes.create_unicode_buffer(261)
                            if kernel32.GetVolumeInformationW(
                                ctypes.c_wchar_p(root), buf, len(buf),
                                None, None, None, None, 0,
                            ):
                                label = buf.value or None
                        except Exception:
                            label = None
                        name = f"{label} ({letter}:)" if label else f"{letter}:"
                        _add_volume(name, root)
                finally:
                    kernel32.SetErrorMode(old_mode)

        if platform.system() == "Darwin":
            _scan_dir("/Volumes")
        elif platform.system() == "Windows":
            _scan_windows_drives()
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

    @blueprint.route("/api/system/info")
    def api_system_info():
        """Return system information: ONNX Runtime, hardware."""
        info = runtime_execution_info()
        import config as cfg
        from platform_support import platform_support_info

        try:
            effective = get_db().get_effective_config(cfg.load())
        except Exception:
            effective = cfg.load()
        info["platform_support"] = platform_support_info(effective)

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

    @blueprint.route("/api/scan/status")
    def api_scan_status():
        db = get_db()

        # DB file size
        db_size = 0
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)

        # Thumbnail cache size
        thumb_dir = config["THUMB_CACHE_DIR"]
        thumb_size = 0
        if os.path.isdir(thumb_dir):
            for f in os.listdir(thumb_dir):
                fp = os.path.join(thumb_dir, f)
                if os.path.isfile(fp):
                    thumb_size += os.path.getsize(fp)

        # ``photo_count`` is the workspace's total inventory (includes photos
        # in folders flagged 'missing'), so the dashboard's headline number
        # stays honest when an external drive is unmounted instead of
        # collapsing to 0. ``accessible_photo_count`` is the actionable
        # subset; when it's lower, the dashboard surfaces the gap.
        # ``keyword_count`` likewise reports inventory-wide so the Keywords
        # card agrees with the Top Species / Other Keywords charts (which
        # also count photos in missing folders).
        return jsonify(
            {
                "photo_count": db.count_photos_in_workspace(),
                "accessible_photo_count": db.count_photos(),
                "missing_folder_count": len(db.get_missing_folders()),
                "folder_count": db.count_folders(),
                "keyword_count": db.count_keywords_in_workspace(),
                "pending_changes": db.count_pending_changes(),
                "db_size": db_size,
                "thumb_cache_size": thumb_size,
            }
        )

    @blueprint.route("/api/logs/stream")
    def api_log_stream():
        """SSE stream of all server log output.

        Auto-closes after 6s of inactivity to prevent stale connections
        from exhausting Flask's thread pool during page navigation.
        The browser's EventSource will auto-reconnect.
        """
        broadcaster = get_log_broadcaster()
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

    @blueprint.route("/api/logs/recent")
    def api_logs_recent():
        count = min(max(1, request.args.get("count", 100, type=int)), 1000)
        return jsonify(get_log_broadcaster().get_recent(count))

    @blueprint.route("/api/report-issue", methods=["POST"])
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
                with open(os.path.join(os.path.dirname(__file__), "..", "..", "pyproject.toml"), "rb") as f:
                    vireo_version = tomllib.load(f)["project"]["version"]
            except Exception:
                vireo_version = "unknown"

        # --- App state ---
        db = None
        try:
            db = get_db()
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
            recent_jobs = get_runner().get_history(db, limit=10)
        except Exception:
            recent_jobs = []

        # --- Config (sanitized) ---
        import config as cfg

        def _redact(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    key = k.lower()
                    if any(s in key for s in ("token", "secret", "password")) or key.endswith("_key"):
                        out[k] = "[REDACTED]"
                    elif any(s in key for s in ("path", "root", "_bin", "directory", "editor")):
                        out[k] = "[REDACTED_PATH]" if v else v
                    else:
                        out[k] = _redact(v)
                return out
            if isinstance(obj, list):
                return [_redact(item) for item in obj]
            return obj

        import config_schema

        # Config strings are kept only where the value is one of a fixed set
        # of identifiers. Every other string (NAS hosts and user names in
        # remote_targets, recent import destinations, output folders, quick
        # filter labels, and any key added later) is replaced: matching key
        # names for secrets and paths let those through whenever a new key
        # was named differently.
        safe_string_settings = {
            key for key, spec in config_schema.SCHEMA.items()
            if spec["type"] == "enum"
        } | {"browse_card_fields", "subject_types"}

        def _redact_config(obj, dotted=""):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    key = str(k).lower()
                    child = f"{dotted}.{k}" if dotted else str(k)
                    if any(s in key for s in ("token", "secret", "password")) or key.endswith("_key"):
                        out[k] = "[REDACTED]"
                    elif any(s in key for s in ("path", "root", "_bin", "directory", "editor")):
                        out[k] = "[REDACTED_PATH]" if v else v
                    else:
                        out[k] = _redact_config(v, child)
                return out
            if isinstance(obj, list):
                return [_redact_config(item, dotted) for item in obj]
            if isinstance(obj, str) and obj:
                if dotted in safe_string_settings or dotted.startswith("keyboard_shortcuts."):
                    return obj
                return "[REDACTED]"
            return obj

        sanitized_config = _redact_config(cfg.load())

        # Exact catalog roots are private and can also appear in logs. Issue
        # reports retain the diagnostic message while replacing those values;
        # the user can paste a path into the description when it is relevant.
        private_paths = []
        if db is not None:
            with contextlib.suppress(Exception):
                private_paths = [
                    row[0] for row in db.conn.execute("SELECT path FROM folders")
                    if row[0]
                ]

        def _sanitize_text(value):
            text = str(value)
            for private_path in sorted(private_paths, key=len, reverse=True):
                text = text.replace(private_path, "[PHOTO_PATH]")
            home = os.path.expanduser("~")
            if home:
                text = text.replace(home, "~")
            return text

        def _sanitize_private_values(value):
            if isinstance(value, dict):
                return {key: _sanitize_private_values(item) for key, item in value.items()}
            if isinstance(value, list):
                return [_sanitize_private_values(item) for item in value]
            if isinstance(value, str):
                return _sanitize_text(value)
            return value

        sanitized_logs = _sanitize_private_values(
            get_log_broadcaster().get_recent(200)
        )

        try:
            from platform_support import filesystem_type, platform_support_info

            filesystems = sorted({
                kind for path in private_paths
                if (kind := filesystem_type(path))
            })
            support_info = _redact(platform_support_info(cfg.load()))
        except Exception:
            filesystems = []
            support_info = {}
        execution_info = {}
        with contextlib.suppress(Exception):
            execution_info = runtime_execution_info()

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
                "inference_device": execution_info.get("device"),
                "inference_providers": execution_info.get("onnxruntime_providers", []),
                "platform_support": support_info,
                "library_filesystems": filesystems,
            },
            "logs": sanitized_logs,
            "app_state": {
                "workspace": ws_name,
                "folders": folder_count,
                "photos": photo_count,
                "predictions": pred_count,
            },
            "recent_jobs": _sanitize_private_values(_redact(recent_jobs)),
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

    return blueprint
