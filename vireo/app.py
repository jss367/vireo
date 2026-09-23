"""Flask web app for the Vireo photo browser.

Usage:
    python vireo/app.py --db ~/.vireo/vireo.db [--port 8080]
"""

import argparse
import concurrent.futures
import functools
import json
import logging
import logging.handlers
import math
import os
import posixpath
import re
import secrets
import stat
import subprocess
import sys
import time
import uuid
import webbrowser
from datetime import UTC, datetime

import id_conflicts
import places
import remote_setup
from best_batch import best_batch_scope, build_best_batch_response
from db import (
    Database,
    IncompatibleDatabaseError,
    MissingPhotosCancelled,
)
from flask import (
    Flask,
    jsonify,
    request,
)
from highlights_payload import (
    apply_highlight_preferences,
    apply_ordered_highlights,
    bucket_best_score,
    build_highlights_payload,
    build_life_list_payload,
    collect_highlight_buckets,
    filter_highlight_curation_state,
    filter_highlight_sections,
    normalize_highlight_confirmation_filter,
    photo_highlight_entries,
    species_canonicalizer,
)
from jobs import JobRunner, LogBroadcaster
from photo_payload import (
    attach_nested_edit_recipes,
)
from preview_cache import (
    reconcile_preview_cache,
)
from proc import no_window_kwargs
from schema import ensure_schema
from services.local_folder import (
    local_root_for_folder,
    local_root_under_folder,
    workspace_ids_for_folder_tree,
)
from services.local_workspace import (
    folder_has_local_workspace,
    stage_boundary_lock,
)
from services.photo_deletion import PhotoDeletion
from services.pipeline_launch import PipelineChain
from services.render_cache import RenderCache, queue_edit_recipe_sync
from services.visual_scope import (
    VISUAL_COLLECTION_MSG,
    VisualScope,
)
from volume_reachability import (  # noqa: F401  (re-exported for tests)
    _NETWORK_PROBE_LOCK,
    _NETWORK_PROBES,
)
from volume_reachability import (
    network_root_reachable as _network_root_reachable,
)
from web import app_hooks
from web import responses as web_responses
from web.audit import create_audit_blueprint
from web.background_jobs import make_background_job
from web.batch import create_batch_blueprint
from web.browse import create_browse_blueprint
from web.caches import create_caches_blueprint
from web.capture_time import create_capture_time_blueprint
from web.card_cleanup import create_card_cleanup_blueprint
from web.collections import create_collections_blueprint
from web.dashboard import create_dashboard_blueprint
from web.duplicates import create_duplicates_blueprint
from web.editing import create_editing_blueprint
from web.encounters import create_encounters_blueprint
from web.export import create_export_blueprint
from web.folders import create_folders_blueprint
from web.highlights import create_highlights_blueprint
from web.history import create_history_blueprint
from web.imports import create_imports_blueprint
from web.inat import InatTokenGeneration, create_inat_blueprint
from web.job_launchers import create_job_launchers_blueprint
from web.jobs import create_jobs_blueprint
from web.keywords import create_keywords_blueprint
from web.life_list import create_life_list_blueprint
from web.local_folder import LOCAL_FOLDER_JOB_TYPES, create_local_folder_blueprint
from web.local_workspace import LOCAL_WORKSPACE_JOB_TYPES, create_local_workspace_blueprint
from web.location_edits import LocationErrors
from web.locations import create_locations_blueprint
from web.media import create_media_blueprint
from web.misses import create_misses_blueprint
from web.models import create_models_blueprint
from web.moves import create_moves_blueprint
from web.pages import create_pages_blueprint
from web.photo_edit_recipes import create_photo_edit_recipes_blueprint
from web.photo_labels import create_photo_labels_blueprint
from web.photo_location_keywords import create_photo_location_keywords_blueprint
from web.photo_review import create_photo_review_blueprint
from web.photos import create_photos_blueprint
from web.pipeline import create_pipeline_blueprint
from web.predictions import create_predictions_blueprint
from web.remote_setup import create_remote_setup_blueprint
from web.request_args import (
    MAX_SELECTION_PHOTOS,
    coerce_collection_id,
    request_flag_filter,
    request_location_status_filter,
)
from web.settings import create_settings_blueprint
from web.species import create_species_blueprint
from web.storage import create_storage_blueprint
from web.sync import create_sync_blueprint
from web.system import create_system_blueprint
from web.workspaces import create_workspace_blueprint
from working_copy_cache import (
    evict_if_over_quota as evict_working_copy_cache_if_over_quota,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# How many planned capture-date folders a date-organized move job snapshots
# into its config for the jobs panel. The panel lists these and reports the
# real total separately, so the route stays readable (and the job row small)
# even when a source folder spans hundreds of dates.
MOVE_DATE_DEST_PREVIEW_LIMIT = 8


# Stable ordering and labels for the palette + nav rendering.
# The `id` is the nav-id used in `tabs`; `href` is the canonical
# route. Labels match what the navbar showed before the unification.
ALL_PAGES = [
    {"id": "import",          "label": "Import",          "href": "/import",
     "keywords": "import add photos card copy ingest new"},
    {"id": "pipeline",        "label": "Process",         "href": "/pipeline",
     "keywords": "process classify detect group stages"},
    {"id": "jobs",            "label": "Jobs",            "href": "/jobs"},
    {"id": "pipeline_review", "label": "Process Review",  "href": "/pipeline/review"},
    {"id": "pipeline_rapid_review", "label": "Rapid Review", "href": "/pipeline/rapid-review"},
    {"id": "review",          "label": "Review",          "href": "/review"},
    {"id": "cull",            "label": "Cull",            "href": "/cull"},
    {"id": "misses",          "label": "Misses",          "href": "/misses"},
    {"id": "highlights",      "label": "Highlights",      "href": "/highlights"},
    {"id": "life_list",       "label": "Life List",       "href": "/life-list"},
    {"id": "browse",          "label": "Browse",          "href": "/browse"},
    {"id": "edit",            "label": "Edit",            "href": "/edit"},
    {"id": "map",             "label": "Map",             "href": "/map"},
    {"id": "location_review", "label": "Review Photo Locations", "href": "/locations/review",
     "keywords": "location review map coordinates collections gps places"},
    {"id": "dashboard",       "label": "Dashboard",       "href": "/dashboard"},
    {"id": "storage",         "label": "Storage",         "href": "/storage"},
    {"id": "audit",           "label": "Audit",           "href": "/audit"},
    {"id": "card_cleanup",    "label": "Card cleanup",    "href": "/card-cleanup",
     "keywords": "card cleanup free space delete verified memory card format sd"},
    {"id": "move",            "label": "Move",            "href": "/move"},
    {"id": "id_conflicts",    "label": "ID Conflicts",    "href": "/id-conflicts",
     "keywords": "compare conflict prediction model disagreement species keyword classify review"},
    {"id": "settings",        "label": "Settings",        "href": "/settings"},
    {"id": "workspace",       "label": "Workspace",       "href": "/workspace"},
    {"id": "lightroom",       "label": "Lightroom",       "href": "/lightroom"},
    {"id": "shortcuts",       "label": "Shortcuts",       "href": "/shortcuts"},
    {"id": "keywords",        "label": "Keywords",        "href": "/keywords"},
    {"id": "duplicates",      "label": "Duplicates",      "href": "/duplicates"},
    {"id": "logs",            "label": "Logs",            "href": "/logs"},
]

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


_FINDER_TRASH_TIMEOUT_SECS = 30
_FINDER_TRASH_BATCH_SIZE = 20
_MOUNT_QUERY_TIMEOUT_SECS = 5

# Distinct from ``None`` so ``_trash_paths`` can tell "caller didn't pass
# network_roots" (re-query is safe) apart from "caller's own mount query
# already failed and it is passing that fail-closed signal through"
# (re-querying would overwrite the caller's classification with a stale
# or empty set the moment the share detaches, reclassifying an
# already-known custom mount point as local and reintroducing the
# unbounded-I/O hang this routing exists to prevent).
_NETWORK_ROOTS_UNSET = object()


class _NetworkVolumeRoots(set):
    """Network roots plus the live /Volumes roots from one mount snapshot."""

    def __init__(self, network_roots=(), mounted_volume_roots=()):
        super().__init__(network_roots)
        self.mounted_volume_roots = frozenset(mounted_volume_roots)


def _mounted_volume_roots(mounts):
    """Return live top-level ``/Volumes/<name>`` roots from parsed mounts."""
    roots = set()
    for mount in mounts:
        normalized = posixpath.normpath(mount["mount_point"])
        parts = normalized.split("/")
        if len(parts) == 3 and parts[1] == "Volumes" and parts[2]:
            roots.add(normalized)
    return roots


def _volume_root_for_path(filepath):
    """Return ``/Volumes/<name>`` for a path on a macOS mounted volume."""
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    parts = normalized.split(os.sep)
    if len(parts) < 4 or parts[0] != "" or parts[1] != "Volumes" or not parts[2]:
        return None
    return os.sep.join(parts[:3])


def _network_volume_roots(run=subprocess.run):
    """Return mounted macOS network-volume roots without touching the shares.

    ``mount`` reads the kernel mount table, so this stays responsive even when
    an SMB server is unhealthy.  ``None`` means the mount table could not be
    read; callers fail closed and treat ``/Volumes`` paths as network-backed
    rather than risking an unbounded in-process filesystem call.
    """
    if sys.platform != "darwin":
        return set()
    try:
        result = run(
            ["mount"], capture_output=True, text=True,
            timeout=_MOUNT_QUERY_TIMEOUT_SECS,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = result.stdout or ""
    mounts = remote_setup.parse_mount_table(output)
    return _NetworkVolumeRoots(
        # ``mount`` always reports macOS/POSIX paths.  Keep parsing independent
        # of the host running the test suite (notably Windows' ``ntpath``).
        (
            posixpath.normpath(mount["mount_point"])
            for mount in mounts
            if remote_setup.mount_type_is_network_or_unknown(
                mount["fs_type"],
            )
        ),
        _mounted_volume_roots(mounts),
    )


def _expand_first_symlink_prefix(filepath):
    """Expand one local symlink prefix without resolving its target.

    ``realpath`` follows the target and can block when that target is an
    unhealthy network share. Reading the first symlink itself only touches
    its local directory entry; the unvisited suffix is then appended
    lexically so network-volume classification stays free of share I/O.
    """
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    drive, tail = os.path.splitdrive(normalized)
    parts = [part for part in tail.split(os.sep) if part]
    prefix = drive + os.sep
    for index, part in enumerate(parts):
        prefix = os.path.join(prefix, part)
        try:
            target = os.readlink(prefix)
        except OSError:
            continue
        if os.name == "nt":
            # Windows junctions commonly expose their substitution path
            # through os.readlink() with an extended-length prefix. Strip it
            # so comparisons against ordinary drive or UNC mount roots use
            # the same spelling.
            if target.startswith("\\\\?\\UNC\\"):
                target = "\\\\" + target[8:]
            elif target.startswith("\\\\?\\"):
                target = target[4:]
        if not os.path.isabs(target):
            target = os.path.join(os.path.dirname(prefix), target)
        return os.path.normpath(os.path.join(target, *parts[index + 1:]))
    return None


def _path_on_network_volume(filepath, network_roots):
    """Whether ``filepath`` should avoid in-process mounted-volume I/O."""
    normalized = os.path.normpath(os.path.abspath(filepath))
    if network_roots is None:
        # Discovery failed, so there is no trustworthy evidence that any
        # candidate is local. Route every macOS path through bounded Finder
        # handling instead of risking an in-process stat on a custom mount.
        # Preserve the explicit /Volumes fallback on non-macOS test hosts.
        return (
            sys.platform == "darwin"
            or _volume_root_for_path(normalized) is not None
        )
    for _depth in range(16):
        for root in network_roots:
            try:
                if os.path.commonpath((normalized, root)) == root:
                    return True
            except ValueError:
                continue
        if sys.platform == "darwin":
            volume_root = _volume_root_for_path(normalized)
            if volume_root is not None:
                mounted_roots = getattr(
                    network_roots, "mounted_volume_roots", None,
                )
                # A detached network mount disappears from the snapshot but
                # leaves its /Volumes directory behind. Conversely, a live
                # local USB/APFS root in the same snapshot must retain the
                # local-trash path. Plain sets from older callers/tests carry
                # no liveness evidence, so continue to fail closed for them.
                if mounted_roots is None or volume_root not in mounted_roots:
                    return True
        if sys.platform != "darwin":
            return False
        expanded = _expand_first_symlink_prefix(normalized)
        if expanded is None:
            return False
        normalized = expanded
    # A symlink loop or unusually deep chain cannot be classified safely.
    # Fail closed on macOS so no subsequent stat reaches a possible share.
    return True


def _deepest_network_root_for_path(filepath, network_roots):
    """Return the *deepest* ``network_roots`` entry ``filepath`` resolves into.

    ``_path_on_network_volume`` answers the yes/no membership question and
    short-circuits on the first matching root, which is enough for routing
    decisions but not for reachability probing.  When mounts are nested —
    e.g. a still-reachable ``/Volumes/NAS`` share with a detached
    ``/Volumes/NAS/archive`` share mounted underneath it — the caller must
    probe reachability of the *exact* mount the path depends on rather than
    any reachable ancestor; otherwise a healthy outer mount would vouch for
    a nested inner mount that is actually gone, and Finder's false
    ``missing`` result would prune catalog rows for photos that reappear on
    reconnect.  Returns ``None`` when no root matches (either directly or
    via symlink expansion, mirroring ``_path_on_network_volume``'s traversal).
    """
    if not network_roots:
        return None
    normalized = os.path.normpath(os.path.abspath(filepath))
    for _depth in range(16):
        best = None
        best_len = -1
        for root in network_roots:
            try:
                if (
                    os.path.commonpath((normalized, root)) == root
                    and len(root) > best_len
                ):
                    # Longest matching root wins so nested mounts probe the
                    # inner share rather than an outer one that happens to
                    # be iterated first from the roots set.
                    best = root
                    best_len = len(root)
            except ValueError:
                continue
        if best is not None:
            return best
        if sys.platform != "darwin":
            return None
        expanded = _expand_first_symlink_prefix(normalized)
        if expanded is None:
            return None
        normalized = expanded
    return None


def _missing_paths_via_finder(filepaths, timeout=_FINDER_TRASH_TIMEOUT_SECS):
    """Boundedly confirm which paths remain absent according to Finder."""
    filepaths = [os.fspath(path) for path in filepaths]
    if not filepaths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "try",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "tell application \"Finder\"",
            "-e", "if exists fileRef then",
            "-e", "set statusValue to \"exists\"",
            "-e", "else",
            "-e", "set statusValue to \"missing\"",
            "-e", "end if",
            "-e", "end tell",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *filepaths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(
            result.stderr.strip()
            or f"Finder existence check failed ({result.returncode})"
        )
    statuses = result.stdout.splitlines()
    if len(statuses) != len(filepaths):
        raise OSError("Finder returned an invalid existence response")
    missing = set()
    existing = set()
    failures = []
    for filepath, outcome in zip(filepaths, statuses, strict=True):
        if outcome == "missing":
            missing.add(filepath)
        elif outcome == "exists":
            existing.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder existence check failed",
            })
    return missing, existing, failures


def _ensure_volume_trashes_dir(filepath, ensured_volumes):
    """Make sure ``/Volumes/<X>/.Trashes/<uid>/`` exists when ``filepath`` is on
    an external/network mount. macOS ``send2trash`` legacy mode raises
    ``OSError: Directory not found`` on volumes where this directory was never
    created (fresh SMB shares, NAS mounts, USB drives) — without it the caller
    falls back to AppleScript Finder, which then times out under load (-1712)
    and stalls the whole bulk trash for hours.

    No-op for paths outside ``/Volumes/`` (the system Trash already handles
    those) and for volumes already ensured this request. ``OSError`` from
    ``makedirs`` (read-only mount, ACL block) is swallowed so the actual
    trash call surfaces a more specific error than "could not mkdir".
    """
    volume_root = _volume_root_for_path(filepath)
    if volume_root is None:
        return
    if volume_root in ensured_volumes:
        return
    ensured_volumes.add(volume_root)
    trashes_dir = os.path.join(volume_root, ".Trashes", str(os.getuid()))
    try:
        os.makedirs(trashes_dir, mode=0o700, exist_ok=True)
    except OSError as exc:
        log.debug("could not ensure %s: %s", trashes_dir, exc)


def _move_to_volume_trash(filepath):
    """Move one file directly into a local mounted volume's Trash directory.

    Finder ultimately performs a same-volume move into
    ``/Volumes/<name>/.Trashes/<uid>``. Doing that move directly avoids the
    legacy Carbon ``send2trash`` failure on removable drives. Network mounts
    must be filtered by :func:`_trash_paths` before calling this helper because
    their rename syscall can block indefinitely. Returns ``True`` only when
    the move completed; callers retain their normal trash fallbacks on
    ``False``.
    """
    if sys.platform != "darwin":
        return False
    volume_root = _volume_root_for_path(filepath)
    if volume_root is None:
        return False

    trash_dir = os.path.join(volume_root, ".Trashes", str(os.getuid()))
    try:
        os.makedirs(trash_dir, mode=0o700, exist_ok=True)
        # Refuse a redirected Trash directory. The destination must remain on
        # the same mounted volume as the source.
        real_trash = os.path.realpath(trash_dir)
        if os.path.commonpath((volume_root, real_trash)) != volume_root:
            raise OSError("volume Trash directory resolves outside its volume")

        basename = os.path.basename(filepath)
        stem, ext = os.path.splitext(basename)
        # Atomically reserve the destination name with O_CREAT|O_EXCL so two
        # concurrent moves for same-named files from different folders can't
        # both observe an empty slot and then rename to the same path — POSIX
        # rename would silently replace one file, permanently losing a photo
        # the user meant to send to Trash.
        candidate = os.path.join(trash_dir, basename)
        reserved = None
        reserved_stat = None
        for _ in range(32):
            try:
                fd = os.open(
                    candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
                )
                try:
                    reserved_stat = os.fstat(fd)
                finally:
                    os.close(fd)
                reserved = candidate
                break
            except FileExistsError:
                candidate = os.path.join(
                    trash_dir, f"{stem} {uuid.uuid4().hex[:8]}{ext}",
                )
        if reserved is None:
            raise OSError(
                f"could not reserve a unique Trash destination in {trash_dir}",
            )
        try:
            # ``os.replace`` (not ``os.rename``) so the O_EXCL placeholder we
            # just reserved is overwritten. POSIX rename replaces silently,
            # but Windows rename raises when the destination exists.
            os.replace(filepath, reserved)
        except OSError:
            # A network filesystem can commit a rename but lose the success
            # response. Never unlink ``reserved`` unless it is still the exact
            # placeholder we created; otherwise that path may now be the photo.
            try:
                current_stat = os.lstat(reserved)
                try:
                    os.lstat(filepath)
                    source_still_exists = True
                except FileNotFoundError:
                    source_still_exists = False
                if not source_still_exists:
                    return True
                placeholder_unchanged = (
                    reserved_stat.st_ino
                    and current_stat.st_dev == reserved_stat.st_dev
                    and current_stat.st_ino == reserved_stat.st_ino
                    and current_stat.st_size == reserved_stat.st_size
                )
                if placeholder_unchanged:
                    os.unlink(reserved)
            except OSError:
                pass
            raise
        return True
    except OSError as exc:
        log.debug("Direct volume Trash move failed for %s: %s", filepath, exc)
        return False


def _trash_via_finder(filepaths, timeout=_FINDER_TRASH_TIMEOUT_SECS):
    """Trash paths via one bounded Finder call with per-item outcomes.

    Fallback for when send2trash fails (e.g. external volumes where the
    legacy Carbon API can't locate .Trashes). macOS-only: on Linux/Windows
    ``send2trash`` already implements the platform trash spec, so there is no
    equivalent fallback. Raising here (instead of spawning a doomed
    ``osascript``) lets the caller surface the original send2trash failure.

    The script catches each path's error and continues so a retry containing
    files already moved by a timed-out earlier batch cannot abort before later
    files. Returns ``(moved_paths, missing_paths, failures)``. A "missing"
    outcome (Finder saw ``sourceExists=false`` but the parent directory still
    exists) is reported separately so the caller can revalidate mount
    identity before accepting it: an unmounted network volume leaves its
    mount-point directory in place on the underlying local FS, so Finder's
    ``parentExists`` check alone cannot distinguish a genuine delete from a
    silently detached mount.
    """
    if sys.platform != "darwin":
        raise OSError("Finder trash fallback is only available on macOS")
    if isinstance(filepaths, (str, bytes, os.PathLike)):
        filepaths = [os.fspath(filepaths)]
    else:
        filepaths = [os.fspath(path) for path in filepaths]
    if not filepaths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "try",
            "-e", "tell application \"Finder\" to delete fileRef",
            "-e", "set statusValue to \"moved\"",
            "-e", "on error",
            "-e", "try",
            "-e", (
                "set parentPath to do shell script \"/usr/bin/dirname \" & "
                "quoted form of (contents of posixPath)"
            ),
            "-e", "set parentRef to POSIX file parentPath",
            "-e", "tell application \"Finder\"",
            "-e", "set sourceExists to exists fileRef",
            "-e", "set parentExists to exists parentRef",
            "-e", "end tell",
            "-e", (
                "if (not sourceExists) and parentExists then "
                "set statusValue to \"missing\""
            ),
            "-e", "end try",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *filepaths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(result.stderr.strip() or f"Finder trash failed ({result.returncode})")
    statuses = result.stdout.splitlines()
    if len(statuses) != len(filepaths):
        raise OSError(
            "Finder trash returned an invalid per-file status response"
        )
    moved_paths = set()
    missing_paths = set()
    failures = []
    for filepath, outcome in zip(filepaths, statuses, strict=True):
        if outcome == "moved":
            moved_paths.add(filepath)
        elif outcome == "missing":
            missing_paths.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder Trash failed",
            })
    return moved_paths, missing_paths, failures


def _snapshot_parent_device(filepath):
    """Return the parent directory's ``st_dev`` for later mount verification.

    Callers snapshot this before any deletion attempt and pass it to
    :func:`_path_confirmed_gone` afterwards. A network mount that
    disconnects mid-op leaves the mount-point directory visible on the
    underlying local filesystem, so ``os.path.isdir`` alone reports the
    parent as live even though the actual volume is gone. Comparing the
    pre-op and post-op ``st_dev`` catches that drop. ``None`` means no
    baseline could be captured, so the device check is skipped.
    """
    parent = os.path.dirname(filepath) or os.sep
    try:
        return os.stat(parent).st_dev
    except OSError:
        return None


def _path_confirmed_gone(filepath, expected_parent_dev=None):
    """Return True only when the file is verifiably absent from a live volume.

    ``os.path.exists`` returning False is ambiguous on network volumes: it
    also returns False when the underlying stat call errors out because the
    mount went away mid-operation. Treating that as "successfully moved to
    Trash" would prune the catalog row for a photo that reappears when the
    mount comes back. Confirm both that the file is missing *and* that the
    parent directory is still reachable so a genuine live-volume delete is
    accepted while a mid-flight disconnect is preserved as a failure.

    When ``expected_parent_dev`` is provided (from
    :func:`_snapshot_parent_device`), also require the parent's current
    ``st_dev`` to match — a network mount that vanishes can leave its
    mount-point directory in place on the underlying local filesystem, so
    the ``os.path.isdir`` check alone would incorrectly accept the file
    as gone.
    """
    if os.path.exists(filepath):
        return False
    parent = os.path.dirname(filepath) or os.sep
    try:
        parent_stat = os.stat(parent)
    except OSError:
        return False
    if not stat.S_ISDIR(parent_stat.st_mode):
        return False
    return not (
        expected_parent_dev is not None
        and parent_stat.st_dev != expected_parent_dev
    )


def _trash_paths(filepaths, progress_callback=None, already_missing_out=None,
                 network_roots=_NETWORK_ROOTS_UNSET):
    """Move paths to Trash and return ``(moved, successful, failures)``.

    Missing paths are successful (the requested end state already holds) but
    are not counted as moved. On macOS mounted volumes we try a direct,
    same-volume rename first. Network volumes skip all in-process move APIs:
    an SMB ``rename(2)`` can wait in the kernel indefinitely, so those paths
    go directly to the time-bounded Finder subprocess. Remaining paths use
    send2trash individually so failures can be attributed, then one Finder
    process per bounded batch.

    ``already_missing_out``, when a mutable set, is populated with every
    path treated as successful because the requested end state already
    held (local preflight found it absent, or Finder reported it missing
    on a still-mounted volume). Callers surface those to users as
    "already missing" rather than as trashed, so a duplicate-cleanup that
    finds every loser already gone can return an explicit terminal
    result instead of a silent ``{trashed: 0}``.

    ``network_roots`` lets a caller reuse a mount-table classification it
    already performed. When omitted we re-query ``_network_volume_roots()``.
    Callers whose own mount query already failed should pass ``None``
    explicitly: that preserves the fail-closed classification they made
    against ``/Volumes`` paths instead of us re-querying and — if the second
    query succeeds with the share now detached — silently reclassifying an
    already-known custom mount point (``/Users/me/mnt/photos``) as local,
    which is the exact case we routed through Finder in the first place.
    """
    ordered = list(dict.fromkeys(filepaths))
    successful = set()
    moved = 0
    fallback = []
    preflight_errors = {}
    finder_candidates = []
    network_finder_candidates = set()
    send_errors = {}
    if network_roots is _NETWORK_ROOTS_UNSET:
        network_roots = _network_volume_roots()
    processed = set()

    def report_processed(filepath):
        if filepath in processed:
            return
        processed.add(filepath)
        if progress_callback:
            progress_callback(
                len(processed), len(ordered), os.path.basename(filepath),
            )

    # Classify paths using the kernel mount table before any source or parent
    # stat.  Those metadata calls can themselves block indefinitely while an
    # unhealthy SMB mount is reconnecting, so network candidates must go
    # straight to the bounded Finder subprocess.
    local_paths = []
    for filepath in ordered:
        if _path_on_network_volume(filepath, network_roots):
            finder_candidates.append(filepath)
            network_finder_candidates.add(filepath)
            send_errors[filepath] = "Network volume Trash operation failed"
        else:
            local_paths.append(filepath)

    # Snapshot each local parent's st_dev before we touch anything. A network
    # mount that vanishes mid-batch can leave the mount-point directory
    # visible on the underlying local FS, so ``os.path.isdir`` alone would
    # accept the file as gone. Comparing pre-op vs post-op st_dev catches
    # the mount drop even when the directory still stats cleanly.
    parent_devs = {path: _snapshot_parent_device(path) for path in local_paths}

    for filepath in local_paths:
        if not os.path.isfile(filepath):
            # ``os.path.isfile`` returning False is ambiguous on network
            # volumes — it also happens when the underlying stat fails
            # because the mount is already disconnected. Only treat the
            # path as "already gone" when the parent directory is still
            # reachable AND its device matches the pre-op snapshot;
            # otherwise preserve as a failure so the caller doesn't prune
            # the catalog row for a photo that reappears when the mount
            # comes back.
            if _path_confirmed_gone(filepath, parent_devs.get(filepath)):
                log.warning("File already missing: %s", filepath)
                successful.add(filepath)
                if already_missing_out is not None:
                    already_missing_out.add(filepath)
            else:
                preflight_errors[filepath] = (
                    "Source path is unreachable"
                )
                log.warning(
                    "Trash preflight: source unreachable for %s", filepath,
                )
            report_processed(filepath)
            continue
        if _move_to_volume_trash(filepath):
            successful.add(filepath)
            moved += 1
            report_processed(filepath)
        else:
            fallback.append(filepath)

    if fallback:
        from send2trash import send2trash as _trash
        for filepath in fallback:
            try:
                _trash(filepath)
                successful.add(filepath)
                moved += 1
                report_processed(filepath)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
                    raise
                # Some platform Trash APIs can complete the move and still
                # report a post-operation error. Only trust the "not there"
                # signal when we can positively verify it — a disconnected
                # network volume also makes ``os.path.exists`` return False
                # (the underlying stat fails), which would otherwise mask a
                # real failure and prune the catalog row for a photo that
                # reappears when the mount comes back.
                if _path_confirmed_gone(filepath, parent_devs.get(filepath)):
                    successful.add(filepath)
                    moved += 1
                    report_processed(filepath)
                    continue
                send_errors[filepath] = str(exc)
                if sys.platform == "darwin":
                    finder_candidates.append(filepath)
                else:
                    report_processed(filepath)

    def _finder_missing_is_trustworthy(
        filepath, current_network_roots, reachable_network_roots,
        confirmed_network_missing, path_to_deepest_root,
    ):
        """Reject Finder's "missing" outcome when the underlying mount is gone.

        Finder reports "missing" when ``sourceExists=false`` but
        ``parentExists=true``. An unmounted network volume leaves its
        mount-point directory in place on the underlying local FS, so the
        parent-exists check alone cannot distinguish a genuine delete from a
        silently detached mount. Accepting "missing" in that case would
        prune the catalog row for a photo that reappears on remount.

        For network-classified paths we require three independent signals:
        (1) the mount table (already re-queried once per Finder batch by
        the caller and passed in via ``current_network_roots``) still
        lists a root the path resolves into; (2) an out-of-process
        ``stat`` probe on the *deepest* matching root the path resolves
        into — separate from Finder's cache — responds within its bounded
        timeout, so a still-listed but unreachable SMB server cannot
        masquerade as "empty" and, crucially, a reachable ancestor mount
        cannot vouch for a nested inner mount that is actually gone; and
        (3) Finder's second-look ``exists`` query also confirmed the path
        as missing. Doing the mount recheck and reachability probes per
        batch instead of per path keeps the worst-case cost bounded — a
        20-item retry of already-moved paths would otherwise spawn one
        ``mount`` (or ``stat``) subprocess per path. For local fallbacks
        we reuse the parent-device snapshot check that guards the
        send2trash path already.
        """
        if filepath in network_finder_candidates:
            if current_network_roots is None:
                # Mount discovery failed on the recheck — we cannot
                # confirm the volume is still mounted, so refuse to trust
                # "missing" and let the caller retry.
                return False
            if not _path_on_network_volume(filepath, current_network_roots):
                return False
            if filepath not in confirmed_network_missing:
                return False
            # Require the *exact* mount the path depends on — not just any
            # reachable ancestor — to respond to the out-of-process stat
            # probe. When a detached inner share is nested beneath a
            # reachable outer share (e.g. ``/Volumes/NAS/archive`` under a
            # still-live ``/Volumes/NAS``), only the deepest match tells us
            # whether the photo could reappear on reconnect.
            deepest_root = path_to_deepest_root.get(filepath)
            if deepest_root is None:
                return False
            return deepest_root in reachable_network_roots
        return _path_confirmed_gone(filepath, parent_devs.get(filepath))

    for finder_batch in _chunked(
        finder_candidates, size=_FINDER_TRASH_BATCH_SIZE,
    ):
        try:
            finder_moved_paths, finder_missing_paths, finder_failures = (
                _trash_via_finder(finder_batch)
            )
            moved += len(finder_moved_paths)
            successful.update(finder_moved_paths)
            # Query the mount table at most once per batch. A retry that
            # contains many paths already moved by an earlier timed-out
            # Finder call comes back with every path in ``missing`` — doing
            # a fresh ``mount`` subprocess per path could otherwise burn up
            # to ``_MOUNT_QUERY_TIMEOUT_SECS`` × ``len(batch)`` seconds and
            # undermine the bounded batch behaviour this code establishes.
            batch_network_missing = any(
                path in network_finder_candidates
                for path in finder_missing_paths
            )
            batch_network_roots = (
                _network_volume_roots() if batch_network_missing else None
            )
            # Probe each still-relevant mount root with a bounded
            # out-of-process ``stat`` — a signal independent of Finder's
            # exists-cache — so a still-listed but unreachable SMB server
            # cannot make ``missing`` outcomes look legitimate. Run the
            # probes concurrently so a Finder batch spanning many
            # unavailable shares completes within one probe timeout
            # rather than accumulating ``len(distinct_roots)`` ×
            # ``_MOUNT_QUERY_TIMEOUT_SECS`` serially — a full 20-item
            # batch across unreachable roots would otherwise add up to
            # ~100 seconds of hang time before the Finder recheck.
            reachable_network_roots = set()
            path_to_deepest_root = {}
            confirmed_network_missing = set()
            finder_recheck_errors = set()
            if batch_network_missing and batch_network_roots is not None:
                paths_still_on_network = {
                    path for path in finder_missing_paths
                    if path in network_finder_candidates
                    and _path_on_network_volume(path, batch_network_roots)
                }
                # Associate each path with the *deepest* mount root it
                # resolves into so nested mounts probe reachability of the
                # inner share rather than an outer one that happens to be
                # iterated first. Set iteration is order-independent, so
                # picking the first match could otherwise validate a
                # detached inner mount using a live outer one and prune
                # rows for photos that reappear on reconnect.
                for path in paths_still_on_network:
                    root = _deepest_network_root_for_path(
                        path, batch_network_roots,
                    )
                    if root is not None:
                        path_to_deepest_root[path] = root
                distinct_roots = list(set(path_to_deepest_root.values()))
                if distinct_roots:
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=len(distinct_roots),
                    ) as executor:
                        probe_results = executor.map(
                            _network_root_reachable, distinct_roots,
                        )
                        for root, is_reachable in zip(
                            distinct_roots, probe_results, strict=True,
                        ):
                            if is_reachable:
                                reachable_network_roots.add(root)
                if paths_still_on_network:
                    try:
                        (
                            confirmed_network_missing,
                            reappeared_paths,
                            recheck_failures,
                        ) = _missing_paths_via_finder(paths_still_on_network)
                        for path in reappeared_paths:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                "Source reappeared during Trash operation"
                            )
                        for failure in recheck_failures:
                            finder_recheck_errors.add(failure["path"])
                            send_errors[failure["path"]] = failure["error"]
                    except subprocess.TimeoutExpired:
                        for path in paths_still_on_network:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                "Finder existence check timed out"
                            )
                    except Exception as exc:
                        for path in paths_still_on_network:
                            finder_recheck_errors.add(path)
                            send_errors[path] = (
                                str(exc) or "Finder existence check failed"
                            )
            for missing_path in finder_missing_paths:
                if _finder_missing_is_trustworthy(
                    missing_path, batch_network_roots,
                    reachable_network_roots,
                    confirmed_network_missing,
                    path_to_deepest_root,
                ):
                    successful.add(missing_path)
                    if already_missing_out is not None:
                        already_missing_out.add(missing_path)
                else:
                    if missing_path not in finder_recheck_errors:
                        send_errors[missing_path] = "Source path is unreachable"
                    log.warning(
                        "Rejecting Finder 'missing' outcome for %s: "
                        "underlying mount appears to have detached",
                        missing_path,
                    )
            for failure in finder_failures:
                send_errors[failure["path"]] = failure["error"]
        except subprocess.TimeoutExpired:
            log.warning(
                "Finder Trash timed out after %ss for %d file(s)",
                _FINDER_TRASH_TIMEOUT_SECS, len(finder_batch),
            )
            for filepath in finder_batch:
                send_errors[filepath] = (
                    f"Finder Trash timed out after "
                    f"{_FINDER_TRASH_TIMEOUT_SECS}s"
                )
        except Exception as exc:
            log.warning("Finder Trash failed for a file batch", exc_info=True)
            for filepath in finder_batch:
                send_errors[filepath] = str(exc) or "Finder Trash failed"
        for filepath in finder_batch:
            report_processed(filepath)

    failures = []
    for filepath in ordered:
        if filepath in successful:
            continue
        error = (
            preflight_errors.get(filepath)
            or send_errors.get(filepath)
            or "Trash operation failed"
        )
        failures.append({"path": filepath, "error": error})
        log.warning("Trash failed for %s: %s", filepath, error)
    return moved, successful, failures


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


def _migrate_edit_math_render_caches(app):
    """Invalidate rendered caches when the edit-math version has bumped.

    Cached previews/thumbnails are keyed by ``(photo_id, size)`` only — the
    edit recipe isn't part of the key. When the per-pixel rendering math in
    ``image_edits`` / ``tone`` changes, the old bytes on disk are no longer
    what we'd produce now, but a recipe-unchanged photo would otherwise keep
    serving them until the user manually cleared the cache.

    On each startup we compare ``db_meta["edit_math_version"]`` against
    ``image_edits.EDIT_MATH_VERSION``. If it lags, we drop:

      * every ``preview_cache`` row plus its on-disk JPEG
      * every per-photo thumbnail file plus its ``photos.thumb_path``

    for photos that have a non-null edit recipe (recipe-free photos render
    identically across math versions and don't need re-rendering). Then we
    write the new version so the migration is a no-op next boot.

    On a fresh DB with no recipes, the walk does nothing and we still bump
    the version so future deploys only act on real prior state.
    """
    from image_edits import EDIT_MATH_VERSION

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    preview_dir = os.path.join(vireo_dir, "previews")
    db = Database(app.config["DB_PATH"])
    try:
        stored = db.get_meta("edit_math_version")
        try:
            stored_version = int(stored) if stored is not None else 1
        except (TypeError, ValueError):
            stored_version = 1
        if stored_version >= EDIT_MATH_VERSION:
            return

        rows = db.conn.execute(
            "SELECT photo_id FROM photo_edit_recipes"
        ).fetchall()
        photo_ids = [row["photo_id"] for row in rows]

        invalidated_previews = 0
        invalidated_thumbs = 0
        # If any unlink fails (locked file on Windows, transient permission
        # error), we must NOT stamp the new version: a stale file/preview_cache
        # row could still be served, and bumping the version would make the
        # next boot skip the migration and never retry. Leaving the version
        # behind makes the migration idempotent and self-retrying.
        purge_failed = False

        # Scan preview_dir once and group untracked preview files by photo id.
        # The per-photo listdir would otherwise be O(N*M) (N edited photos x M
        # preview files) and can spend minutes just rescanning the same
        # directory on a large library before the server even starts.
        edited_set = set(photo_ids)
        untracked_previews_by_pid = {}
        try:
            preview_names = os.listdir(preview_dir)
        except FileNotFoundError:
            # Cache dir doesn't exist yet — nothing untracked to clean up.
            preview_names = ()
        except OSError:
            # Permissions / locked network volume / other transient read
            # failure: we can't see what's in there, so we don't know whether
            # there are stale orphans to purge. Skip the scan but leave the
            # version old so the next boot retries — matches the unlink-error
            # contract instead of taking the app down for a disposable cache
            # problem.
            log.warning(
                "Failed to list preview cache dir %s during edit-math "
                "migration; leaving version at %s to retry next boot",
                preview_dir, stored_version, exc_info=True,
            )
            preview_names = ()
            purge_failed = True

        for name in preview_names:
            if not name.endswith(".jpg"):
                continue
            underscore = name.find("_")
            if underscore <= 0:
                continue
            try:
                file_pid = int(name[:underscore])
            except ValueError:
                continue
            if file_pid in edited_set:
                untracked_previews_by_pid.setdefault(file_pid, []).append(name)
        for pid in photo_ids:
            for row in db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id = ?", (pid,)
            ).fetchall():
                size_value = row["size"]
                path = os.path.join(preview_dir, f"{pid}_{size_value}.jpg")
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    log.warning(
                        "Failed to remove stale preview cache %s during "
                        "edit-math migration", path, exc_info=True,
                    )
                    purge_failed = True
                    continue
                db.conn.execute(
                    "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
                    (pid, size_value),
                )
                invalidated_previews += 1
            for name in untracked_previews_by_pid.get(pid, ()):
                path = os.path.join(preview_dir, name)
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    purge_failed = True
            thumb_cache = os.path.join(thumb_dir, f"{pid}.jpg")
            try:
                if os.path.exists(thumb_cache):
                    os.remove(thumb_cache)
                    invalidated_thumbs += 1
            except OSError:
                log.warning(
                    "Failed to remove stale thumbnail %s during "
                    "edit-math migration", thumb_cache, exc_info=True,
                )
                purge_failed = True
                continue
            for source in ("raw", "jpeg"):
                variant = os.path.join(thumb_dir, f"{pid}_{source}.jpg")
                try:
                    if os.path.exists(variant):
                        os.remove(variant)
                        invalidated_thumbs += 1
                except OSError:
                    log.warning(
                        "Failed to remove paired thumbnail %s during "
                        "edit-math migration", variant, exc_info=True,
                    )
                    purge_failed = True
            db.conn.execute(
                "UPDATE photos SET thumb_path = NULL WHERE id = ?", (pid,),
            )

        if purge_failed:
            # Commit the row deletions that did succeed, but leave the stored
            # version unchanged so the next boot re-runs and retries the
            # paths that couldn't be purged this time.
            db.conn.commit()
            log.warning(
                "edit_math_version %s -> %s: some cache purges failed; "
                "leaving version at %s so the migration retries next boot "
                "(invalidated %d preview-cache entries, %d thumbnails so far)",
                stored_version, EDIT_MATH_VERSION, stored_version,
                invalidated_previews, invalidated_thumbs,
            )
            return

        db.set_meta("edit_math_version", EDIT_MATH_VERSION, _commit=False)
        db.conn.commit()

        if photo_ids:
            log.info(
                "edit_math_version %s -> %s: invalidated %d preview-cache "
                "entries and %d thumbnails across %d edited photos",
                stored_version, EDIT_MATH_VERSION,
                invalidated_previews, invalidated_thumbs, len(photo_ids),
            )
        else:
            log.info(
                "edit_math_version %s -> %s: no edited photos, nothing to "
                "invalidate", stored_version, EDIT_MATH_VERSION,
            )
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _migrate_unedited_raw_preview_sources(app):
    """Drop previews that may have used an edit-quality RAW working copy.

    RAW working copies deliberately preserve highlight headroom and therefore
    look flatter/darker than the camera-rendered rendition used for browsing.
    Older preview routing treated that working copy as the canonical source
    even when a RAW had no edit recipe.  Those bytes are keyed only by
    ``(photo_id, size)``, so fixing source selection alone would keep serving
    already-cached dark tiers indefinitely.

    Purge only recipe-free RAWs that currently have a working copy, and gate
    the migration in ``db_meta`` so large libraries pay the directory scan
    once.  If any cache file cannot be inspected or removed, leave the marker
    unset so the next launch retries instead of permanently adopting stale
    pixels.
    """
    from image_loader import RAW_EXTENSIONS

    marker = "unedited_raw_camera_preview_source_v1"
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    db = Database(app.config["DB_PATH"])
    try:
        if db.get_meta(marker) == "1":
            return

        rows = db.conn.execute(
            """SELECT p.id, p.filename
               FROM photos p
               LEFT JOIN photo_edit_recipes r ON r.photo_id = p.id
               WHERE p.working_copy_path IS NOT NULL
                 AND r.photo_id IS NULL"""
        ).fetchall()
        affected = {
            int(row["id"])
            for row in rows
            if os.path.splitext(row["filename"] or "")[1].lower()
            in RAW_EXTENSIONS
        }

        purge_failed = False
        names_by_pid = {}
        try:
            preview_names = os.listdir(preview_dir)
        except FileNotFoundError:
            preview_names = ()
        except OSError:
            log.warning(
                "Failed to list preview cache dir %s while migrating "
                "unedited RAW preview sources; retrying next launch",
                preview_dir,
                exc_info=True,
            )
            preview_names = ()
            purge_failed = True

        for name in preview_names:
            if not name.endswith(".jpg"):
                continue
            underscore = name.find("_")
            if underscore <= 0:
                continue
            try:
                photo_id = int(name[:underscore])
            except ValueError:
                continue
            if photo_id in affected:
                names_by_pid.setdefault(photo_id, set()).add(name)

        invalidated = 0
        for photo_id in affected:
            tracked = db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id=?",
                (photo_id,),
            ).fetchall()
            names = names_by_pid.get(photo_id, set())
            names.update(
                f"{photo_id}_{row['size']}.jpg" for row in tracked
            )
            photo_failed = False
            for name in names:
                path = os.path.join(preview_dir, name)
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    log.warning(
                        "Failed to remove stale unedited RAW preview %s",
                        path,
                        exc_info=True,
                    )
                    photo_failed = True
                    purge_failed = True
            if photo_failed:
                continue
            cursor = db.conn.execute(
                "DELETE FROM preview_cache WHERE photo_id=?", (photo_id,)
            )
            invalidated += max(cursor.rowcount, 0)

        if purge_failed:
            db.conn.commit()
            return

        db.set_meta(marker, "1", _commit=False)
        db.conn.commit()
        if affected:
            log.info(
                "Invalidated %d preview-cache entries across %d unedited "
                "RAW photos so camera-rendered previews can regenerate",
                invalidated,
                len(affected),
            )
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _enforce_preview_cache_quota_at_startup(app):
    """Reconcile and evict at startup so prior runs / external deletes
    can't leave the table out of sync or over quota.

    Reconcile first: if a previous session left ghost rows (files
    deleted while cache was under quota), eviction would see inflated
    totals and stay asleep when it shouldn't, or wake up and run a
    no-op pass over rows whose files don't exist.
    """
    from preview_cache import evict_if_over_quota

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    db = Database(app.config["DB_PATH"])
    try:
        reconcile_preview_cache(db, vireo_dir)
        evict_if_over_quota(db, vireo_dir)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _enforce_working_copy_cache_quota_at_startup(app):
    """Apply the persistent working-copy ceiling when the app starts.

    Quota eviction records a source-mtime marker on each removed row so later
    scans do not immediately regenerate files that were deliberately removed.

    Sweeps ``.<id>.render.*.jpg.tmp`` orphans in ``working/`` first so a
    process kill during a prior on-demand extraction does not permanently
    consume disk outside the configured ceiling (quota accounting skips these
    files by design, so the sweep is their only cleanup path). Passes
    ``startup=True`` so a legacy ``working/<id>.jpg`` whose mtime happens to
    fall inside the concurrent-writer grace window is still reclaimed on the
    first pass — no cache writer can be active this early.
    """
    from working_copy_cache import sweep_abandoned_render_tempfiles

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    sweep_abandoned_render_tempfiles(vireo_dir)
    db = Database(app.config["DB_PATH"])
    try:
        evict_working_copy_cache_if_over_quota(db, vireo_dir, startup=True)
    finally:
        try:
            db.conn.close()
        except Exception:
            pass


def _sweep_abandoned_transient_originals(app):
    """Reclaim non-cacheable ``/original`` renditions the process orphaned.

    When ``_serve_generated_original`` streams a rendition too large for the
    quota (or when the quota is zero), it moves the file into
    ``<vireo_dir>/originals/.<id>.transient.*.jpg`` and unlinks it in the
    generator's ``finally`` block after streaming. A process kill or crash
    during that stream leaves the ``.transient.*.jpg`` behind: working-copy
    eviction only scans ``working/`` and cannot see it, so repeated
    interrupted requests can accumulate arbitrary bytes outside the quota
    with no other cleanup path.
    """
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    originals_dir = os.path.join(vireo_dir, "originals")
    if not os.path.isdir(originals_dir):
        return
    try:
        with os.scandir(originals_dir) as entries:
            for entry in entries:
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                name = entry.name
                if not (
                    name.startswith(".")
                    and ".transient." in name
                    and name.endswith(".jpg")
                ):
                    continue
                try:
                    os.remove(entry.path)
                except OSError as exc:
                    log.warning(
                        "Could not remove abandoned transient rendition %s: %s",
                        entry.path, exc,
                    )
    except OSError as exc:
        log.warning(
            "Could not scan originals directory for transient renditions %s: %s",
            originals_dir, exc,
        )


def _scan_metadata_warning():
    """Thin wrapper over ``metadata.scan_metadata_warning`` for the scan paths.

    Kept as a module-level alias so callers don't import ``metadata`` directly
    at every call site; the implementation lives in ``metadata`` so the
    pipeline-job module can share it.
    """
    from metadata import scan_metadata_warning
    return scan_metadata_warning()


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
    app.config["COMPUTATION_CACHE_DIR"] = os.path.expanduser(
        "~/.vireo/computation-cache"
    )
    app.config["THUMB_CACHE_DIR"] = thumb_cache_dir or os.path.expanduser(
        "~/.vireo/thumbnails"
    )
    app.config["API_TOKEN"] = api_token
    app.config["TRUSTED_HOSTS"] = ["localhost", "127.0.0.1", "::1"]
    app.config["BROWSER_SESSION_COOKIE"] = "vireo_session"
    app.config["BROWSER_SESSION_TOKEN"] = secrets.token_urlsafe(32)
    app.config["BROWSER_AUTH_ENABLED"] = (
        os.environ.get("VIREO_DISABLE_BROWSER_AUTH") != "1"
    )
    app.config["REQUIRE_EXIFTOOL_FOR_IMPORT"] = (
        os.environ.get("VIREO_REQUIRE_EXIFTOOL_FOR_IMPORT", "1") != "0"
    )
    app.config["CARD_CLEANUP_DIR"] = os.path.join(
        os.path.dirname(os.path.abspath(db_path)), "card_cleanup"
    )
    # Built here rather than on first request: a check-then-set in the request
    # path lets two concurrent first requests each build their own store, and a
    # token handed out by whichever store loses the race is never found again.
    # The endpoint would then silently re-derive the whole comparison — the
    # multi-second cost this store exists to avoid.
    app.config["ID_CONFLICTS_SNAPSHOTS"] = id_conflicts.SnapshotStore()

    # Schema creation and migrations are startup work, never request work.
    # `:memory:` is the development exception because each SQLite connection
    # owns a distinct database and therefore must initialize itself.
    ensure_schema(db_path)

    _migrate_legacy_preview_cache(app)
    _migrate_edit_math_render_caches(app)
    _migrate_unedited_raw_preview_sources(app)
    _enforce_preview_cache_quota_at_startup(app)
    _sweep_abandoned_transient_originals(app)
    _enforce_working_copy_cache_quota_at_startup(app)

    # Response helpers and request hooks live in web.responses and
    # web.app_hooks; these names stay bound here because every blueprint
    # factory receives them.
    json_error = web_responses.json_error
    _photo_not_found_error = web_responses.photo_not_found_error
    _request_flag_filter = request_flag_filter
    _request_location_status_filter = request_location_status_filter
    _get_db = functools.partial(app_hooks.get_request_db, db_path)
    # Endpoints that skip the workspace mutation reservation. Mutable: the
    # /api/v1 alias loop at the end of create_app adds ``v1_<view>`` for
    # every aliased endpoint listed here, so headless clients get the same
    # exemptions.
    _reservation_exempt_endpoints = set(app_hooks.RESERVATION_EXEMPT_ENDPOINTS)
    app_hooks.register_app_hooks(
        app,
        get_db=_get_db,
        reservation_exempt_endpoints=_reservation_exempt_endpoints,
    )

    _MAX_PER_PAGE = 500

    # Location error responses shared by the location, place, and batch
    # routes (``web.location_edits``); built once around ``json_error``.
    location_errors = LocationErrors(
        json_error=json_error, photo_not_found_error=_photo_not_found_error,
    )

    # Shared prologue for routes that launch background jobs. See
    # web/background_jobs.py: the decorated view receives a ``JobLaunch``
    # (runner + active workspace + worker-thread db factory) as its first
    # argument and returns ``ctx.start(job_type, work, ...)``.
    background_job = make_background_job(
        lambda: app._job_runner, _get_db, db_path, Database
    )

    # Render/preview cache invalidation lives in services.render_cache;
    # the aliases keep the blueprint wiring below unchanged.
    render_cache = RenderCache(app.config)
    _invalid_preview_cache_paths = render_cache.invalid_preview_cache_paths
    _clear_preview_cache_invalid = render_cache.clear_preview_cache_invalid
    _invalidate_photo_render_cache = render_cache.invalidate_photo_render_cache
    _queue_edit_recipe_sync = queue_edit_recipe_sync

    # Batch delete and the post-delete cache sweep live in
    # services/photo_deletion.py. The filesystem helpers are wrapped in
    # lambdas so they are looked up on this module at call time — tests
    # monkeypatch ``app._trash_paths`` / ``app._chunked`` and must keep
    # reaching the delete path.
    photo_deletion = PhotoDeletion(
        app.config,
        chunked=lambda *args, **kwargs: _chunked(*args, **kwargs),
        trash_paths=lambda *args, **kwargs: _trash_paths(*args, **kwargs),
        snapshot_parent_device=(
            lambda *args, **kwargs: _snapshot_parent_device(*args, **kwargs)
        ),
        path_confirmed_gone=(
            lambda *args, **kwargs: _path_confirmed_gone(*args, **kwargs)
        ),
    )
    _cleanup_cached_files_for_deleted_photos = (
        photo_deletion.cleanup_cached_files_for_deleted_photos
    )
    _run_batch_delete = photo_deletion.run_batch_delete

    # Load user config (e.g. HF token) on startup
    import config as cfg

    startup_cfg = cfg.load()
    if startup_cfg.get("hf_token"):
        os.environ["HF_TOKEN"] = startup_cfg["hf_token"]

    # Initialize job runner, log broadcaster, and default collections
    _t0 = time.time()
    init_db = Database(
        db_path,
        initialize_schema=(db_path == ":memory:"),
    )
    log.info("Database init took %.2fs (workspace: %s)", time.time() - _t0,
             init_db.get_workspace(init_db._active_workspace_id)["name"])
    # File-backed startup skips Database's schema initialization. Repair old
    # move parentage here too, before Browse can serve the stale hierarchy.
    init_db.repair_stale_folder_parents()
    # Migrate the legacy 'Needs Classification' default collection BEFORE
    # seeding defaults — otherwise create_default_collections inserts
    # 'Needs Identification' first, then the migration skips renaming
    # because the target name already exists, leaving a duplicate.
    init_db.migrate_default_subject_collection()
    init_db.migrate_default_needs_identification_collection()
    init_db.migrate_default_location_collections()
    # One-shot keyword-name normalization backfill. Database.__init__ only
    # runs it when initialize_schema=True, and every file-backed connection
    # this app opens — including this startup init_db and every per-request
    # connection at `_get_db` — passes initialize_schema=False. Without an
    # explicit run here, an upgraded DB can serve requests with `‘apapane`-
    # style variant rows still present until some background job happens
    # to construct a full `Database()` (initialize_schema=True); in that
    # window an add/rename can miss the legacy row and create duplicate
    # tags or stale XMP. The method is idempotent (db_meta-gated) so
    # subsequent boots are a cheap SELECT.
    init_db.normalize_keyword_data()
    repaired_location_ancestors = init_db.repair_misclassified_location_ancestors()
    if repaired_location_ancestors:
        log.info(
            "Restored %d location hierarchy nodes misclassified as taxonomy",
            repaired_location_ancestors,
        )
    # Ungroup legacy bursts whose stored votes span more than one species.
    # Those rows display one species and, through accept_prediction's
    # vote-winner lookup, tag another; the repair makes them read as the
    # current classifier would have written them. Runs before the first
    # request so no page can render a row this is about to change, and
    # before any accept can act on one. Depends on no taxonomy or config,
    # so unlike the duplicate-species repair it needs no deferral: it is
    # db_meta-gated and self-logging, and later boots pay one marker
    # lookup. Method logs its own totals — the counts are the point, per
    # CORE_PHILOSOPHY.md.
    init_db.repair_mixed_species_prediction_groups()

    # Parsing taxonomy.json is expensive for a full iNaturalist download.
    # Cache the startup instance so overlapping one-time migrations and the
    # immediate background species pass do not each parse it independently.
    _taxonomy_not_loaded = object()
    _startup_taxonomy = _taxonomy_not_loaded

    def _load_startup_taxonomy():
        nonlocal _startup_taxonomy
        # Do not cache a miss: a concurrent first-run taxonomy download can
        # make the file available before the background retry starts.
        if (
            _startup_taxonomy is _taxonomy_not_loaded
            or _startup_taxonomy is None
        ):
            from taxonomy import load_local_taxonomy

            _startup_taxonomy = load_local_taxonomy()
        return _startup_taxonomy

    def _sync_mark_species_only(db, log_label):
        """Load taxonomy and run mark_species_keywords synchronously.

        Returns True when the pass ran (marking either updated rows or
        found nothing to update), False when taxonomy is missing or the
        pass raised. Callers use the return value to gate follow-up work
        that depends on hierarchy leaves being correctly typed as
        taxonomy/is_species.
        """
        tax = _load_startup_taxonomy()
        if tax is None:
            log.debug(
                "[%s] taxonomy not loaded; deferring species marking",
                log_label,
            )
            return False
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info(
                    "[%s] Marked %d keywords as species from taxonomy",
                    log_label, updated,
                )
            return True
        except Exception:
            log.debug(
                "[%s] mark_species_keywords failed",
                log_label, exc_info=True,
            )
            return False

    # Remove same-photo, same-taxon duplicate associations left by the old
    # hierarchy-import + top-level-confirmation interaction. Idempotent and
    # db_meta-gated, so later boots only pay for a single marker lookup.
    #
    # The repair identifies duplicates via
    # ``(is_species = 1 OR type = 'taxonomy') AND (rank = 'species' OR
    # taxon_id IS NULL)``. On upgraded databases a hierarchical species
    # leaf can still be a plain/general row until mark_species_keywords
    # retypes it, so run marking synchronously first — otherwise the
    # repair query cannot see the leaf, removes nothing, and still stamps
    # its one-shot marker; a subsequent background mark_species_keywords
    # pass could then make the leaf eligible while the redundant root
    # association remains permanently skipped. When taxonomy isn't
    # loaded yet (or marking fails), defer the repair to a later boot
    # rather than stamping the marker over an unmarked hierarchy.
    duplicate_repair_key = Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY
    duplicate_repair_pending = init_db.get_meta(duplicate_repair_key) != "1"
    if duplicate_repair_pending:
        # The legacy bug always left a typed top-level species/taxonomy tag
        # beside another tag on the same photo. If no such pair exists, the
        # repair is structurally impossible and it is safe to stamp the
        # one-shot marker without parsing a potentially huge taxonomy file.
        possible_duplicate = init_db.conn.execute(
            """SELECT 1
               FROM photo_keywords species_pk
               JOIN keywords species_k
                 ON species_k.id = species_pk.keyword_id
               WHERE (species_k.is_species = 1
                      OR species_k.type = 'taxonomy')
                 AND EXISTS (
                     SELECT 1
                     FROM photo_keywords other_pk
                     WHERE other_pk.photo_id = species_pk.photo_id
                       AND other_pk.keyword_id != species_pk.keyword_id
                 )
               LIMIT 1"""
        ).fetchone()
        if possible_duplicate is None:
            init_db.set_meta(duplicate_repair_key, "1")
            duplicate_repair_pending = False
            log.info(
                "Skipped duplicate-species startup repair: "
                "no possible duplicate associations"
            )

    if (
        duplicate_repair_pending
        and _sync_mark_species_only(init_db, "sync-startup-species-mark")
    ):
        init_db.repair_duplicate_photo_species()
    # One-time rewrite of the previous miss-threshold defaults (0.25 / 0.15)
    # to the new defaults (0.20 / 0.12) in both ~/.vireo/config.json and
    # workspace overrides. Gated by a marker so it runs once; re-saved
    # legacy values are preserved on subsequent boots.
    cfg.migrate_legacy_miss_thresholds(init_db)
    # One-time rewrite of the previous eye-focus detection default from on to
    # off in both global config and workspace overrides.
    cfg.migrate_eye_detect_default_off(init_db)
    # One-time resolution of the browse.toggle_ui="h" default clashing with
    # any pre-existing browse binding on ``h``. Writes an explicit "" so the
    # user's existing action keeps working; they can re-bind toggle_ui from
    # the shortcuts editor.
    cfg.migrate_toggle_ui_h_conflict()
    # Existing users commonly have the previous Browse card defaults persisted
    # verbatim. Add the new coordinate-source field only for that exact legacy
    # list; customized card layouts remain unchanged.
    cfg.migrate_browse_location_status_field()
    # One-time rename of the "compare" navigation shortcut to "id_conflicts"
    # after the Compare page became ID Conflicts, so a user's saved binding
    # follows the page instead of being orphaned.
    cfg.migrate_compare_nav_id_to_id_conflicts()
    # One-time rewrite of the previous encounter-grouping species weight
    # (0.10) to the new default (0.40) in both ~/.vireo/config.json and
    # workspace overrides. Without this, upgraded installs that had the
    # pipeline block persisted verbatim keep grouping distinct species into
    # one encounter — the intended split behavior only reaches fresh
    # configs. Only the exact legacy value is rewritten; re-saved values
    # are preserved on subsequent boots.
    cfg.migrate_legacy_w_species_default(init_db)
    # One-time rewrite of the global pipeline.default_strategy (legacy
    # hardcoded strategy name) to pipeline.default_process_id (saved_processes
    # id). The workspace-side rewrite happens inside Database(); this covers
    # the global config file so workspaces that inherit the global default
    # don't silently fall back to import-only after upgrade.
    #
    # init_db uses initialize_schema=False for boot perf, so on the first
    # boot after upgrade the saved_processes table isn't guaranteed to
    # exist yet on this connection — the migration would silently defer
    # and any import in this session that would inherit the legacy global
    # default falls back to import-only until the *next* boot. Open a
    # short-lived schema-initializing handle so the migration completes on
    # the very first boot instead. Only pay the schema-init cost if the
    # migration hasn't been stamped yet; subsequent boots short-circuit
    # inside the function and pass ``init_db`` (whose schema state is
    # irrelevant because the marker check runs first).
    if (
        cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID
        not in cfg._migrations_applied(cfg._read_raw())
    ):
        _default_strategy_migration_db = Database(db_path)
        try:
            cfg.migrate_default_strategy_to_process_id(
                _default_strategy_migration_db,
            )
        finally:
            _default_strategy_migration_db.close()
    else:
        cfg.migrate_default_strategy_to_process_id(init_db)
    init_db.create_default_collections_for_all_workspaces()

    # Keep taxonomy typing and duplicate-species repair fresh in the
    # background. Wildlife classification eligibility is stored separately
    # on photos; species marking no longer materializes a Wildlife keyword.
    import threading

    def _retire_wildlife_genre():
        """Run the catalog-wide XMP migration outside startup readiness.

        Large upgraded catalogs can require tens of thousands of sidecar
        reads here. Keeping that work on create_app's calling thread prevents
        the HTTP listener from binding and makes the desktop launcher report
        a false startup failure when its readiness deadline expires.
        """
        retirement_db = None
        started_at = time.time()
        try:
            retirement_db = Database(db_path)
            retired = retirement_db.retire_builtin_wildlife_genre()
            if retired:
                log.info(
                    "Retired the built-in Wildlife genre from %d photo(s)",
                    retired,
                )
            log.info(
                "Wildlife genre retirement finished in %.2fs",
                time.time() - started_at,
            )
            return retired
        except Exception:
            log.exception("Wildlife genre retirement failed")
            return 0
        finally:
            if retirement_db is not None:
                retirement_db.close()

    # Tests and one-shot tools can invoke the pass deterministically without
    # enabling production timers. Production schedules it only after every
    # route has been registered, immediately before create_app returns.
    app._retire_wildlife_genre = _retire_wildlife_genre

    def _mark_species_and_repair(db, log_label):
        """Load taxonomy, mark species keywords, and repair duplicates."""
        tax = _load_startup_taxonomy()
        if tax is None:
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
            repaired = db.repair_duplicate_photo_species()
            if repaired:
                log.info(
                    "[%s] Removed %d duplicate root species associations",
                    log_label, repaired,
                )
        except Exception:
            log.debug(
                "[%s] species marking/repair failed", log_label, exc_info=True,
            )

    def _mark_species():
        bg_db = None
        try:
            bg_db = Database(db_path)
        except Exception:
            log.debug("Could not open background db for species marking", exc_info=True)
            return
        try:
            _mark_species_and_repair(bg_db, "background")
        finally:
            bg_db.close()

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        threading.Thread(target=_mark_species, daemon=True).start()

    def _folder_health_loop():
        """Periodically check folder health."""
        import time as _time
        _time.sleep(30)  # Initial delay
        while True:
            health_db = None
            try:
                health_db = Database(db_path)
                changed = health_db.check_folder_health()
                if changed:
                    log.info("Folder health check: %d folder(s) changed status", changed)
                    # A background ok↔missing flip would otherwise leave a
                    # ready /api/photos/missing cache serving the pre-flip
                    # photo list: the modal/banner could offer to delete
                    # rows whose folder just went offline, or hide ghosts
                    # from a folder that just came back, until a later
                    # rescan replaced the entry.
                    _invalidate_missing_originals_cache()
            except Exception:
                log.debug("Folder health check failed", exc_info=True)
            finally:
                if health_db is not None:
                    health_db.close()
            _time.sleep(600)  # 10 minutes

    # Suppressed in tests via ``VIREO_DISABLE_STARTUP_BACKFILL_TIMERS``: the
    # ``app_and_db`` fixture seeds folders at fictional paths like
    # ``/photos/2024`` that don't exist on disk. After the 30s grace period
    # this loop calls ``check_folder_health`` on the tmp_path DB, sees the
    # paths missing, and flips folder status to ``'missing'`` — which causes
    # ``get_photos`` to filter the seeded photos out and any subsequent
    # assertion against them to fail with ``IndexError``. On the slow
    # Windows CI runner the full suite takes ~48 min, so by the time the
    # later predictions/photos tests reach the fixture the timer has long
    # since fired.
    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        threading.Thread(target=_folder_health_loop, daemon=True).start()

    app._job_runner = JobRunner(db=init_db)

    # XMP sidecars are read-modify-written files; serialize sync jobs so
    # repeated clicks cannot race while touching the same sidecar.
    app._sync_job_lock = threading.Lock()
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    def _cleanup_app_resources(job_timeout=10.0):
        try:
            jobs_stopped = app._job_runner.shutdown(timeout=job_timeout)
        except Exception:
            jobs_stopped = False
            log.exception("Failed to shut down background jobs cleanly")
        try:
            app._log_broadcaster.uninstall()
        except Exception:
            log.exception("Failed to uninstall log broadcaster during cleanup")
        try:
            init_db.close()
        except Exception:
            log.exception("Failed to close database during cleanup")
        return jobs_stopped

    app._cleanup_app_resources = _cleanup_app_resources

    # Live progress of the most recent new-images walk, keyed by
    # (db_path, workspace_id). Written by the walk's progress callback and
    # read by the GET/POST endpoints so a ``pending`` response can say
    # "38,000 files checked, 2,100 new so far" instead of a bare spinner —
    # the transparency the banner-click path needs on multi-minute walks
    # over large network volumes. Values are per-spawn dicts; a new walk
    # replaces the key wholesale, so readers never see torn state.
    app._new_images_walk_progress = {}
    app._missing_originals_lock = threading.Lock()
    app._missing_originals_cache = {}
    app._missing_originals_inflight = {}
    app._missing_originals_errors = {}
    # Monotonic per-key counter bumped whenever the cache is invalidated
    # while a scan is in flight. Each scan snapshots this at start; if the
    # counter has advanced by the time it finishes, the scan's results are
    # from a pre-invalidation view of the library and must be discarded so
    # deleted photos don't reappear in the banner/modal.
    app._missing_originals_generation = {}

    # Working copies are generated by imports, scans, and on-demand reads.
    # Do not warm the library-wide cache at startup: it consumes disk and
    # CPU for photos the user has not requested.

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

        tpdb = None
        try:
            tpdb = Database(db_path)
            candidate_count = thumb_path_backfill_candidate_count(
                tpdb, app.config["THUMB_CACHE_DIR"],
            )
        except Exception:
            log.exception("thumb_path backfill: candidate check failed")
            return
        finally:
            if tpdb is not None:
                tpdb.close()
        if candidate_count == 0:
            log.debug("thumb_path backfill: no candidates, skipping")
            return

        runner = app._job_runner
        cache_dir = app.config["THUMB_CACHE_DIR"]

        def work(job):
            thread_db = Database(db_path)
            try:
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

                def status_cb(message, **_phase):
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
            finally:
                thread_db.close()

        try:
            runner.start(
                "thumb_path_backfill", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start thumb_path backfill job")

    app._kickoff_thumb_path_backfill = _kickoff_thumb_path_backfill

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        _thumb_backfill_timer = threading.Timer(6.0, _kickoff_thumb_path_backfill)
        _thumb_backfill_timer.daemon = True
        _thumb_backfill_timer.start()

    # -- Page routes --

    app.register_blueprint(create_pages_blueprint(_get_db))

    # -- API routes --

    # Resolves visual-search clauses; owns the per-app query-text
    # embedding cache, so every route shares one instance.
    visual_scope = VisualScope()

    _MISSING_ORIGINALS_STALE_SECONDS = 30 * 60
    _MISSING_ORIGINALS_BACKOFF_SECONDS = 30 * 60
    _MISSING_ORIGINALS_HEAVY_JOB_TYPES = {
        "scan",
        "pipeline",
        "thumbnails",
        "previews",
        "move-photos",
        "move-folder",
        "sync",
        "classify",
        "precompute-embeddings",
        "cull",
        "develop",
        "extract-masks",
        "regroup",
        "import",
        "import-full",
        "import-in-place",
        "ingest",
        "import-photos",
        "batch-delete",
        "duplicate-scan",
        "offline-cache",
        # Navbar's new-images probe walks the same folders a missing-originals
        # scan would; letting them run concurrently can double the filesystem
        # load on slow NAS/SMB libraries.
        "new_images_walk",
        # Folder-scoped and workspace-wide missing-originals scans have
        # distinct cache keys, so the same-key in-flight coalescing does
        # not catch a workspace scan started while a folder scan is
        # running (or vice versa). Treat any in-flight
        # missing_originals_scan as heavy work so automatic reruns
        # don't kick off a second filesystem walk over the same tree.
        "missing_originals_scan",
        # audit.verify_hashes walks every workspace source file and
        # hashes readable ones — the same NAS/SMB trees a Missing
        # Originals scan touches. Letting the 30-minute automatic
        # missing-originals timer fire during verification would
        # double the I/O on those slow volumes.
        "verify-hashes",
        # Card cleanup reads only archive copies that match one card, but
        # those reads still hit the same NAS/SMB trees.
        "card-cleanup-verify",
    }

    def _utc_iso_now():
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _parse_missing_originals_folder_id(db):
        folder_id = request.args.get("folder_id")
        if request.is_json:
            body = request.get_json(silent=True) or {}
            if "folder_id" in body:
                folder_id = body.get("folder_id")
        if folder_id in (None, ""):
            return None
        try:
            folder_id = int(folder_id)
        except (TypeError, ValueError):
            raise ValueError("folder_id must be an integer") from None
        linked = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (db._active_workspace_id, folder_id),
        ).fetchone()
        if not linked:
            raise LookupError("folder not found")
        return folder_id

    def _missing_originals_key(db, folder_id):
        return (db._db_path, db._active_workspace_id, folder_id)

    def _missing_originals_payload(db, folder_id):
        key = _missing_originals_key(db, folder_id)
        now = time.monotonic()
        with app._missing_originals_lock:
            entry = app._missing_originals_cache.get(key)
            inflight = app._missing_originals_inflight.get(key)
            err = app._missing_originals_errors.get(key)
            # An error recorded after the last cached scan means a later
            # refresh failed. Returning the pre-refresh photo list as a
            # fresh "ready" result would hide the failure — and worse,
            # let the user delete rows whose originals may have been
            # restored between scans. When a scan is in flight the UI
            # already shows "pending", so still surface the stale
            # entry then; otherwise prefer the error state.
            cache_superseded_by_error = (
                entry is not None
                and err is not None
                and not inflight
                and err["set_at"] > entry["set_at"]
            )
            if entry is not None and not cache_superseded_by_error:
                status = "pending" if inflight else "ready"
                photos = entry["photos"]
                checked_at = entry["checked_at"]
                stale = now - entry["set_at"] > _MISSING_ORIGINALS_STALE_SECONDS
                error = None
            elif inflight:
                status = "pending"
                photos = []
                checked_at = None
                stale = False
                error = None
            elif err is not None:
                status = "error"
                photos = []
                checked_at = err["checked_at"]
                stale = False
                error = err["error"]
            else:
                status = "not_ready"
                photos = []
                checked_at = None
                stale = False
                error = None
            backoff_seconds = 0
            if err is not None:
                backoff_seconds = max(0, int(err["backoff_until"] - now))
            return {
                "status": status,
                "pending": bool(inflight),
                "checked_at": checked_at,
                "stale": stale,
                "error": error,
                "job_id": inflight if isinstance(inflight, str) else None,
                "photos": photos,
                "backoff_seconds": backoff_seconds,
                "workspace_id": db._active_workspace_id,
                "folder_id": folder_id,
            }

    def _build_missing_originals_rows(
        db,
        folder_id=None,
        progress_callback=None,
        cancel_callback=None,
    ):
        thumb_dir = app.config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_dir)
        preview_dir = os.path.join(vireo_dir, "previews")
        working_dir = os.path.join(vireo_dir, "working")

        def check_cancelled():
            if cancel_callback is not None and cancel_callback():
                raise MissingPhotosCancelled("missing originals scan cancelled")

        # Index preview cache once. The endpoint is polled from the navbar,
        # so per-photo `glob(preview_dir, f"{pid}_*.jpg")` was O(missing ×
        # cache_size) readdirs per request. Build {pid} from a single
        # listdir and check the set in O(1) per row.
        preview_pids: set[int] = set()
        try:
            check_cancelled()
            with os.scandir(preview_dir) as it:
                for entry in it:
                    check_cancelled()
                    name = entry.name
                    # Match `{id}.jpg` (legacy full preview) or `{id}_{size}.jpg`
                    # (sized variant). Anything else is not part of the per-photo
                    # cache and should be ignored.
                    if not name.endswith(".jpg"):
                        continue
                    head = name[:-4].split("_", 1)[0]
                    if head.isdigit():
                        preview_pids.add(int(head))
        except FileNotFoundError:
            pass  # cache dir hasn't been created yet — no previews

        out = []
        for row in db.get_missing_photos(
            folder_id=folder_id,
            progress_callback=progress_callback,
            cancel_callback=cancel_callback,
        ):
            check_cancelled()
            pid = row["id"]
            src = os.path.join(row["folder_path"], row["filename"])
            stem, _ext = os.path.splitext(src)
            # Working copy: the DB path wins when set, but legacy rows from
            # before working_copy_path was tracked can still have a file at
            # the default <vireo>/working/<id>.jpg location — and the batch
            # delete path cleans that up regardless of the DB column. If we
            # only consulted the column the badge would lie about what's
            # about to be removed.
            wc_rel = row["working_copy_path"]
            default_wc = os.path.join(working_dir, f"{pid}.jpg")
            if wc_rel:
                has_wc = os.path.isfile(os.path.join(vireo_dir, wc_rel))
            else:
                has_wc = os.path.isfile(default_wc)
            out.append({
                "id": pid,
                "filename": row["filename"],
                "extension": row["extension"],
                "folder_id": row["folder_id"],
                "folder_path": row["folder_path"],
                "timestamp": row["timestamp"],
                "file_size": row["file_size"],
                "has_thumb": os.path.isfile(os.path.join(thumb_dir, f"{pid}.jpg")),
                "has_preview": pid in preview_pids,
                "has_working_copy": has_wc,
                "has_xmp_sidecar": (
                    os.path.isfile(stem + ".xmp")
                    or os.path.isfile(stem + ".XMP")
                    or os.path.isfile(src + ".xmp")
                    or os.path.isfile(src + ".XMP")
                ),
            })
        attach_nested_edit_recipes(db, out)
        return out

    def _missing_originals_heavy_job_active():
        for job in app._job_runner.list_jobs():
            if job.get("status") not in (
                "running", "pausing", "paused", "queued",
            ):
                continue
            if job.get("type") in _MISSING_ORIGINALS_HEAVY_JOB_TYPES:
                return True
        return False

    def _pending_local_workspace_transition(workspace_id, db=None):
        """Return the queued/running local-workspace transition job, or None.

        ``db``: pass an explicit Database when calling off the request
        thread (job threads have no request context); defaults to the
        request-scoped db via ``_get_db()``.
        """
        # ``has_local_workspace`` only observes the ``local_workspaces`` row
        # a stage worker inserts once it actually runs; a stage/sync/discard
        # that has been enqueued but not yet reached that insert would leave
        # the row absent. A scan or move-folder enqueued in that window
        # passes its own guard and then rebases the catalog after the
        # transition worker later claims the workspace, so the folder /
        # workspace_folders / folders rows those jobs write end up outside
        # the manifest and local_workspace_folders. Detecting the pending
        # transition job in the runner queue closes that race at enqueue.
        if workspace_id is None:
            return None
        for job in app._job_runner.list_jobs():
            if job.get("status") not in (
                "queued", "running", "pausing", "paused",
            ):
                continue
            job_type = job.get("type")
            if job_type in LOCAL_WORKSPACE_JOB_TYPES and job.get("workspace_id") == workspace_id:
                return job
            if job_type in LOCAL_FOLDER_JOB_TYPES:
                if job.get("workspace_id") == workspace_id:
                    return job
                config = job.get("config") or {}
                root_ids = (config.get("root_folder_ids") or []) if isinstance(config, dict) else []
                if db is None:
                    db = _get_db()
                if any(
                    workspace_id in workspace_ids_for_folder_tree(db, int(root_id))
                    for root_id in root_ids
                ):
                    return job
        return None

    def _invalidate_missing_originals_cache(workspace_ids=None):
        """Drop cached Missing Originals results for this app's database.

        Photos are shared across workspaces (a folder can be linked into
        more than one), so a photo-row removal must clear every
        workspace cache that could still list it — scoping to the
        active workspace lets other workspaces keep serving stale
        ready payloads until their next scan.

        ``workspace_ids`` narrows the invalidation to those workspace
        ids. Use it on workspace create/delete to clear entries that
        could otherwise be served to a later workspace that reuses a
        SQLite rowid.
        """
        ws_filter = None if workspace_ids is None else {int(w) for w in workspace_ids}
        with app._missing_originals_lock:
            for store in (
                app._missing_originals_cache,
                app._missing_originals_errors,
            ):
                for key in list(store.keys()):
                    if key[0] != db_path:
                        continue
                    if ws_filter is not None and key[1] not in ws_filter:
                        continue
                    store.pop(key, None)
            # Bump generation for every in-flight scan under this DB so
            # its completion path refuses to write its stale
            # pre-invalidation snapshot back into the cache.
            for key in list(app._missing_originals_inflight.keys()):
                if key[0] != db_path:
                    continue
                if ws_filter is not None and key[1] not in ws_filter:
                    continue
                app._missing_originals_generation[key] = (
                    app._missing_originals_generation.get(key, 0) + 1
                )

    def _start_missing_originals_scan(db, folder_id=None, automatic=False):
        key = _missing_originals_key(db, folder_id)
        scan_started_at = time.monotonic()
        now = scan_started_at
        token = object()
        suppressed_reason = None
        reuse_existing = False
        fresh_cache = False
        with app._missing_originals_lock:
            inflight = app._missing_originals_inflight.get(key)
            if inflight:
                reuse_existing = True
            entry = app._missing_originals_cache.get(key)
            # Gate on when the last scan STARTED, not when it finished. The
            # navbar re-arms its 30-minute automatic timer from POST time,
            # so a scan that takes real wall-clock time to walk the disk
            # leaves ``set_at`` well under the threshold when the next tick
            # arrives — every other automatic scan would otherwise be
            # skipped, and deletions could stay undiscovered for nearly an
            # hour. Legacy entries without ``started_at`` fall back to
            # ``set_at``.
            if (
                not reuse_existing
                and automatic
                and entry is not None
                and now - entry.get("started_at", entry["set_at"])
                < _MISSING_ORIGINALS_STALE_SECONDS
            ):
                fresh_cache = True
            err = app._missing_originals_errors.get(key)
            if (
                not reuse_existing
                and not fresh_cache
                and automatic
                and err is not None
                and now < err["backoff_until"]
            ):
                suppressed_reason = "backoff"
        if reuse_existing:
            return _missing_originals_payload(db, folder_id)
        if fresh_cache:
            return _missing_originals_payload(db, folder_id)
        if automatic and suppressed_reason is None and _missing_originals_heavy_job_active():
            suppressed_reason = "heavy_job_active"
        if suppressed_reason is not None:
            payload = _missing_originals_payload(db, folder_id)
            payload["suppressed"] = True
            payload["reason"] = suppressed_reason
            if suppressed_reason == "heavy_job_active":
                payload["status"] = "skipped"
            return payload
        scan_generation = 0
        with app._missing_originals_lock:
            inflight = app._missing_originals_inflight.get(key)
            if inflight:
                reuse_existing = True
            else:
                app._missing_originals_inflight[key] = token
                scan_generation = app._missing_originals_generation.get(key, 0)
        if reuse_existing:
            return _missing_originals_payload(db, folder_id)

        runner = app._job_runner
        ws_id = db._active_workspace_id
        db_file = db._db_path
        scope_label = "workspace" if folder_id is None else f"folder #{folder_id}"

        def work(job):
            thread_db = None
            try:
                thread_db = Database(db_file)
                if ws_id is not None:
                    thread_db.set_active_workspace(ws_id)

                def progress(payload):
                    current = int(payload.get("photos_considered") or 0)
                    total = int(payload.get("total_photos") or 0)
                    missing_found = int(payload.get("missing_found") or 0)
                    folders_checked = int(payload.get("folders_checked") or 0)
                    current_folder = payload.get("current_folder") or ""
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    job["progress"]["current_file"] = current_folder
                    phase = (
                        f"{folders_checked:,} folders checked, "
                        f"{current:,} photos considered, "
                        f"{missing_found:,} missing"
                    )
                    runner.push_event(job["id"], "progress", {
                        "current": current,
                        "total": total,
                        "current_file": current_folder,
                        "folders_checked": folders_checked,
                        "missing_found": missing_found,
                        "phase": phase,
                    })

                def cancel_check():
                    return runner.is_cancelled(job["id"])

                photos = _build_missing_originals_rows(
                    thread_db,
                    folder_id=folder_id,
                    progress_callback=progress,
                    cancel_callback=cancel_check,
                )
                if cancel_check():
                    return {"cancelled": True, "scope": scope_label}
                checked_at = _utc_iso_now()
                stale = False
                with app._missing_originals_lock:
                    current_gen = app._missing_originals_generation.get(key, 0)
                    if cancel_check():
                        return {"cancelled": True, "scope": scope_label}
                    if current_gen != scan_generation:
                        # A batch delete (or other invalidation) fired
                        # while this scan was walking the disk. Its photo
                        # list reflects the pre-delete library, so writing
                        # it back would resurrect just-removed photos in
                        # the banner. Drop the result and let the next
                        # scan recompute.
                        stale = True
                    else:
                        app._missing_originals_cache[key] = {
                            "photos": photos,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "started_at": scan_started_at,
                        }
                        app._missing_originals_errors.pop(key, None)
                return {
                    "missing_count": len(photos),
                    "checked_at": checked_at,
                    "scope": scope_label,
                    "stale": stale,
                }
            except MissingPhotosCancelled:
                raise
            except Exception as exc:
                checked_at = _utc_iso_now()
                with app._missing_originals_lock:
                    current_gen = app._missing_originals_generation.get(key, 0)
                    if current_gen == scan_generation:
                        app._missing_originals_errors[key] = {
                            "error": str(exc) or exc.__class__.__name__,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "backoff_until": (
                                time.monotonic()
                                + _MISSING_ORIGINALS_BACKOFF_SECONDS
                            ),
                        }
                raise
            finally:
                if thread_db is not None:
                    thread_db.close()
                with app._missing_originals_lock:
                    if app._missing_originals_inflight.get(key) in (
                        token,
                        job["id"],
                    ):
                        app._missing_originals_inflight.pop(key, None)

        try:
            job_id = runner.start(
                "missing_originals_scan",
                work,
                workspace_id=ws_id,
                config={"scope": scope_label, "folder_id": folder_id},
                ephemeral=False,
                counts_for_badge=True,
            )
        except Exception:
            with app._missing_originals_lock:
                if app._missing_originals_inflight.get(key) is token:
                    app._missing_originals_inflight.pop(key, None)
            raise

        with app._missing_originals_lock:
            if app._missing_originals_inflight.get(key) is token:
                app._missing_originals_inflight[key] = job_id
        payload = _missing_originals_payload(db, folder_id)
        if payload.get("status") != "ready":
            payload["job_id"] = job_id
            payload["pending"] = True
            payload["status"] = "pending"
        return payload









    def _normalize_photo_id_list(raw_ids):
        """Validate and de-dupe a JSON ``photo_ids`` list, preserving order."""
        if not isinstance(raw_ids, list) or not raw_ids:
            return None, json_error("photo_ids required", 400)
        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return None, json_error("photo_ids must contain only integers", 400)
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return None, json_error("photo_ids required", 400)
        return photo_ids, None

    def _gps_location_chunks(values, size=800):
        values = list(values)
        for idx in range(0, len(values), size):
            yield values[idx:idx + size]

    def _location_keyword_photo_ids(db, photo_ids):
        """Return ids that already have any linked location keyword."""
        if not photo_ids:
            return set()
        found = set()
        for chunk in _gps_location_chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                "SELECT DISTINCT pk.photo_id "
                "FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE k.type = 'location' AND pk.photo_id IN ({placeholders})",
                chunk,
            ).fetchall()
            found.update(row["photo_id"] for row in rows)
        return found

    _REVERSE_GEOCODE_CACHE_LANGUAGE_KEY = "_vireo_result_language"

    def _google_reverse_geocode(lat, lng, api_key, language):
        """Reverse-geocode while preserving the wrapper's default-on API."""
        if language == "en":
            return places.reverse_geocode(lat, lng, api_key)
        return places.reverse_geocode(lat, lng, api_key, language=None)

    _REVERSE_GEOCODE_CACHE_LEGACY = object()

    def _decode_cached_reverse_geocode(cached, language):
        """Return ``(matches_language, details)`` for a cached response.

        Rows written before the language field existed have no
        ``_vireo_result_language`` key at all; treat those as compatible
        with the current preference so a rollout does not silently
        invalidate every previously cached lookup — and, when no API
        key is configured, force the caller into the ``no_api_key``
        branch instead of reusing the cached result. Post-PR writes
        always include the key (``null`` for the opt-out preference,
        ``"en"`` for the default), so subsequent preference changes
        still invalidate correctly.
        """
        try:
            details = json.loads(cached["response"] or "{}")
        except (ValueError, TypeError):
            details = {}
        if not isinstance(details, dict):
            details = {}
        cached_language = details.pop(
            _REVERSE_GEOCODE_CACHE_LANGUAGE_KEY,
            _REVERSE_GEOCODE_CACHE_LEGACY,
        )
        if cached_language is _REVERSE_GEOCODE_CACHE_LEGACY:
            return True, details
        return cached_language == language, details

    def _encode_cached_reverse_geocode(details, language):
        """Serialize details with the requested language for cache matching.

        The language key is always written — including as ``null`` for
        the opt-out preference — so the decoder can distinguish a
        legacy untagged row from a row explicitly written under
        ``language=None``.
        """
        payload = dict(details) if isinstance(details, dict) else {}
        payload[_REVERSE_GEOCODE_CACHE_LANGUAGE_KEY] = language
        return json.dumps(payload)

    def _resolve_exif_place_for_photo(
        db, photo, api_key, language, grid_cache,
    ):
        """Resolve one photo's EXIF coordinates into normalized place details.

        Returns ``(details, reason)`` where ``details`` is the normalized
        Google-place dict on success and ``reason`` is a short unresolved code
        on failure. Results are de-duped by the DB's reverse-geocode grid so a
        burst in the same cell only performs/cache-checks one lookup.
        """
        lat = photo["latitude"]
        lng = photo["longitude"]
        if lat is None or lng is None:
            return None, "missing_gps"
        try:
            lat = float(lat)
            lng = float(lng)
        except (TypeError, ValueError):
            return None, "invalid_gps"
        if not (math.isfinite(lat) and math.isfinite(lng)):
            return None, "invalid_gps"

        grid = Database._reverse_geocode_grid(lat, lng)
        if grid in grid_cache:
            return grid_cache[grid]

        cached = db.reverse_geocode_cache_get(lat, lng)
        if cached is not None:
            language_matches, details = _decode_cached_reverse_geocode(
                cached, language,
            )
            if language_matches:
                if cached["place_id"] is None:
                    result = (None, "no_match")
                    grid_cache[grid] = result
                    return result
                if not details.get("place_id"):
                    details["place_id"] = cached["place_id"]
                if details.get("place_id"):
                    result = (details, None)
                else:
                    result = (None, "no_match")
                grid_cache[grid] = result
                return result

        if not api_key:
            result = (None, "no_api_key")
            grid_cache[grid] = result
            return result

        try:
            details = _google_reverse_geocode(
                lat, lng, api_key, language,
            )
        except places.PlacesTransientError:
            app.logger.warning(
                "bulk reverse_geocode transient failure for photo=%s lat=%s lng=%s",
                photo["id"],
                lat,
                lng,
            )
            result = (None, "transient_error")
            grid_cache[grid] = result
            return result

        cache_place_id = details.get("place_id") if details else None
        db.reverse_geocode_cache_put(
            lat,
            lng,
            place_id=cache_place_id,
            response_json=_encode_cached_reverse_geocode(details, language),
        )
        if not details or not details.get("place_id"):
            result = (None, "no_match")
        else:
            result = (details, None)
        grid_cache[grid] = result
        return result

    def _bulk_gps_location_source_ids(db, body):
        """Return source photo ids from either ``photo_ids`` or ``collection_id``."""
        raw_ids = body.get("photo_ids")
        if raw_ids:
            return _normalize_photo_id_list(raw_ids)

        collection_id = coerce_collection_id(body.get("collection_id"))
        if collection_id is False:
            return None, json_error("collection_id must be an integer", 400)
        if collection_id is None:
            return None, json_error("photo_ids or collection_id required", 400)

        row = db.conn.execute(
            "SELECT id, visual_json FROM collections "
            "WHERE id = ? AND workspace_id = ?",
            (collection_id, db._ws_id()),
        ).fetchone()
        if row is None:
            return None, json_error("collection not found", 404)
        # get_collection_photo_ids evaluates ``rules`` only; a visual-only
        # collection would silently expand to every metadata match. The
        # picker filters these out, but reject here as the boundary.
        if row["visual_json"] is not None:
            return None, json_error(VISUAL_COLLECTION_MSG, 400)
        return db.get_collection_photo_ids(collection_id), None

    def _bulk_gps_location_payload(db, body, cancel_check=None):
        """Build preview/apply data for resolving locations from EXIF GPS."""
        photo_ids, error = _bulk_gps_location_source_ids(db, body)
        if error is not None:
            return None, error
        if not photo_ids:
            return {
                "total": 0,
                "resolvable": 0,
                "updated": 0,
                "groups": [],
                "unresolved": [],
                "skipped": [],
            }, None
        photos_map = db.get_photos_by_ids(photo_ids)
        if len(photos_map) != len(photo_ids):
            return None, json_error("One or more photos were not found", 404)
        for pid in photo_ids:
            edit_error = location_errors.photo_location_edit_error(db, pid)
            if edit_error is not None:
                return None, edit_error

        assigned_ids = _location_keyword_photo_ids(db, photo_ids)
        import config as cfg
        maps_config = cfg.load()
        api_key = (maps_config.get("google_maps_api_key", "") or "").strip()
        language = places.result_language(maps_config)

        grid_cache = {}
        groups = {}
        unresolved = []
        skipped = []
        ordered_group_keys = []
        cancelled = False
        for pid in photo_ids:
            if cancel_check is not None and cancel_check():
                cancelled = True
                break
            photo = photos_map[pid]
            if pid in assigned_ids:
                skipped.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": "already_has_location",
                })
                continue
            details, reason = _resolve_exif_place_for_photo(
                db, photo, api_key, language, grid_cache,
            )
            if reason is not None:
                unresolved.append({
                    "photo_id": pid,
                    "filename": photo["filename"],
                    "reason": reason,
                })
                continue

            place_id = details.get("place_id")
            if place_id not in groups:
                groups[place_id] = {
                    "place_id": place_id,
                    "summary": _summarize_details(details),
                    "name": details.get("name") or "",
                    "details": details,
                    "photo_ids": [],
                    "sample_filenames": [],
                }
                ordered_group_keys.append(place_id)
            group = groups[place_id]
            group["photo_ids"].append(pid)
            if len(group["sample_filenames"]) < 3:
                group["sample_filenames"].append(photo["filename"])

        group_list = []
        for place_id in ordered_group_keys:
            group = groups[place_id]
            group_list.append({
                "place_id": group["place_id"],
                "summary": group["summary"],
                "name": group["name"],
                "count": len(group["photo_ids"]),
                "photo_ids": group["photo_ids"],
                "sample_filenames": group["sample_filenames"],
            })

        result = {
            "total": len(photo_ids),
            "resolvable": sum(group["count"] for group in group_list),
            "updated": 0,
            "groups": group_list,
            "unresolved": unresolved,
            "skipped": skipped,
            "_details_by_place_id": {k: v["details"] for k, v in groups.items()},
        }
        if cancelled:
            result["cancelled"] = True
        return result, None

    # -- Edit API routes --

        return jsonify({"ok": True})

    def _effective_category_resolver(db, photo_ids):
        """Build ``(photo_id, species) -> category`` against *current* keywords.

        ``predictions.category`` is a snapshot of how the prediction compared
        to the photo's keywords at classification time, and nothing rewrites
        it when keywords change afterwards (the only writers are the classify
        path and duplicate merge). So a photo that gained a Robin keyword
        after a pending Sparrow prediction was stored as ``new`` still reads
        ``new`` — and Browse would offer a bare Accept that tags a species
        conflicting with what the photo already says. ``CORE_PHILOSOPHY.md``
        forbids exactly that: the button must mean what the user reads it as.

        Returns ``match``/``new``/``refinement``/``broader``/``conflict`` from
        ``compare_prediction_to_keywords`` — Compare's vocabulary, because
        this is Compare's computation, shared rather than reimplemented (see
        ``api_predictions_compare``). Callers treat
        ``refinement``/``broader``/``conflict`` as ambiguous, the same set
        Browse's ``predictionIsAmbiguous`` refuses to offer a bare Accept for.

        Two details are load-bearing and are the reason this goes through the
        same helpers Compare uses rather than a raw keyword query:

        * ``get_species_keywords_for_photos`` canonicalizes a hierarchy alias
          through its linked taxon's root, and ``resolve_species_display_name``
          does the same for the prediction label. Comparing raw
          ``keywords.name`` text would make a photo tagged with the leaf
          ``Desert Verdin`` read as *conflicting* with a ``Verdin``
          prediction whenever the taxonomy file is unavailable — inventing an
          ambiguity and sending a settled photo to Review.
        * the comparison runs on the species the accept path would actually
          apply (the burst consensus), not the row's own label.

        Returns None when no comparison is possible (compare or the photo set
        unavailable) so callers can fall back to the stored snapshot.
        """
        photo_ids = [pid for pid in dict.fromkeys(photo_ids) if pid is not None]
        if not photo_ids:
            return None
        try:
            from compare import compare_prediction_to_keywords
        except Exception:
            return None
        # Cached by mtime inside load_local_taxonomy, so this is a lookup on
        # the hot path rather than a re-parse per request. None degrades
        # compare_prediction_to_keywords to exact-text matching, which is
        # still current-state truth — better than a stale column either way,
        # and a missing or corrupt taxonomy file must never hard-fail the
        # endpoint.
        try:
            from taxonomy import load_local_taxonomy
            taxonomy = load_local_taxonomy()
        except Exception:
            taxonomy = None
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        species_by_photo = db.get_species_keywords_for_photos(photo_ids, include_identities=True)
        resolved = {}
        cache = {}

        def comparison_name(species, identity=None):
            identity = identity or resolver.display(species)
            if identity.scientific_name:
                return identity.scientific_name
            if species not in resolved:
                resolved[species] = db.resolve_species_display_name(identity.display_name)
            return resolved[species]

        keyword_names = {}
        for photo_id, entries in species_by_photo.items():
            names = []
            for entry in entries:
                source = {"taxon_id": int(entry["key"][6:])} if entry["key"].startswith("taxon:") else None
                identity = resolver.resolve(entry["name"], source=source) if source else resolver.display(entry["name"])
                names.append(comparison_name(entry["name"], identity))
            keyword_names[photo_id] = names

        def _category(photo_id, species, identity=None):
            if not species or photo_id is None:
                return None
            identity = identity or resolver.display(species)
            key = (photo_id, identity.key)
            if key not in cache:
                if any(entry["key"] == identity.key for entry in species_by_photo.get(photo_id, [])):
                    cache[key] = "match"
                else:
                    comparison = compare_prediction_to_keywords(
                        comparison_name(species, identity),
                        keyword_names.get(photo_id, []),
                        taxonomy,
                    )
                    cache[key] = (
                        comparison.get("category")
                        if isinstance(comparison, dict) else None
                    )
            return cache[key]

        return _category

    _EFFECTIVE_AMBIGUOUS_CATEGORIES = frozenset(
        {"refinement", "broader", "conflict"}
    )
    # Stored-snapshot categories that mean the same thing, used only when no
    # fresh comparison is available.
    _STORED_AMBIGUOUS_CATEGORIES = frozenset({"disagreement", "refinement"})

    def _prediction_is_ambiguous(effective_category, stored_category):
        """Would a bare Accept here be dishonest?

        The fresh comparison wins outright when there is one. ORing it with
        the stored snapshot would make ambiguity a one-way ratchet: a photo
        whose conflicting keyword has since been removed would keep being
        routed to Review forever, naming a conflict that no longer exists —
        the same staleness bug in the other direction. The snapshot is the
        fallback for when no fresh comparison could be made at all.
        """
        if effective_category is not None:
            return effective_category in _EFFECTIVE_AMBIGUOUS_CATEGORIES
        return stored_category in _STORED_AMBIGUOUS_CATEGORIES

    def _ambiguous_prediction_ids(db, rows):
        """Which of ``rows`` a bare Accept must not act on.

        The one definition of "ambiguous" for the pair of endpoints that
        need it: the selection panel, which splits its payload into
        ``acceptable_prediction_ids`` and ``ambiguous_prediction_ids``, and
        ``batch-accept``, which re-derives the same verdict before writing.
        Two conditions, both of which mean a bare Accept would decide
        something the user has not been shown:

        * an ``alternative`` sibling on the row's ``(detection, model)`` —
          the classifier offered a runner-up, so accepting picks a winner on
          the user's behalf;
        * a disagreement/refinement against the photo's species keywords,
          judged by ``_prediction_is_ambiguous`` on the *current* keywords
          (see ``_effective_category_resolver`` for why the stored
          ``category`` column cannot be trusted for this).

        ``batch-accept`` recomputes rather than trusting the payload because
        the panel's split is a snapshot: a keyword added from Review, a
        second Browse tab, or an XMP sync between render and click makes a
        row ambiguous while it is still ``pending``, so the decided-status
        precondition alone cannot catch it. The panel's own refresh handles
        mutations inside one document; only the server sees the rest. This
        lives here — not once per endpoint — for the reason rounds 7 and 8
        established for the status precondition and the accept scope: a rule
        with two implementations is a rule that drifts.

        ``rows`` are prediction rows carrying ``id``, ``photo_id``,
        ``detection_id``, ``model``, ``category``, ``species``, ``group_id``
        and ``individual``. Returns the ambiguous subset of their ids.
        """
        rows = list(rows)
        if not rows:
            return set()
        photo_ids = list(dict.fromkeys(
            row["photo_id"] for row in rows if row["photo_id"] is not None
        ))
        # Keyed by (detection, model) exactly as /api/predictions nests
        # alternatives, so "has alternatives" means the same thing in Browse,
        # in this check, and in Review.
        alt_keys = {
            (row["detection_id"], row["model"])
            for row in db.get_predictions(
                photo_ids=photo_ids, status="alternative",
            )
        }
        effective_category_of = _effective_category_resolver(db, photo_ids)
        from species_identity import SpeciesResolver
        resolver = SpeciesResolver(db=db)
        ambiguous = set()
        for row in rows:
            # Compared on the species the accept path would actually apply
            # (the burst consensus), not the row's own label.
            identity = resolver.consensus(row)
            species = identity.display_name
            effective_category = (
                effective_category_of(row["photo_id"], species, identity)
                if effective_category_of is not None and species else None
            )
            if (
                (row["detection_id"], row["model"]) in alt_keys
                or _prediction_is_ambiguous(effective_category, row["category"])
            ):
                ambiguous.add(row["id"])
        return ambiguous


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


    # -- Statistics --


    # -- Highlights --

    _build_highlights_payload = build_highlights_payload
    _build_life_list_payload = build_life_list_payload

    app.register_blueprint(
        create_highlights_blueprint(
            _get_db,
            json_error,
            build_highlights_payload=_build_highlights_payload,
            chunked=_chunked,
            species_canonicalizer=species_canonicalizer,
            collect_highlight_buckets=collect_highlight_buckets,
            normalize_highlight_confirmation_filter=(
                normalize_highlight_confirmation_filter
            ),
            filter_highlight_sections=filter_highlight_sections,
            apply_ordered_highlights=apply_ordered_highlights,
            apply_highlight_preferences=apply_highlight_preferences,
            filter_highlight_curation_state=filter_highlight_curation_state,
            bucket_best_score=bucket_best_score,
        )
    )

    app.register_blueprint(
        create_local_workspace_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            os.path.dirname(app.config["THUMB_CACHE_DIR"]),
            invalidate_missing_originals=lambda ws_id: _invalidate_missing_originals_cache(
                workspace_ids=[ws_id]
            ),
        )
    )
    app.register_blueprint(
        create_local_folder_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            os.path.dirname(app.config["THUMB_CACHE_DIR"]),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_jobs_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            lambda: app.config["THUMB_CACHE_DIR"],
        )
    )

    # -- Detection API routes --



    def _read_raw_config_file():
        """Return the parsed contents of ~/.vireo/config.json, or {}.

        Unlike cfg.load(), this does NOT merge DEFAULTS — so it contains
        only the keys the user has actually set. Used by write paths so the
        on-disk file stays minimal.

        Preserves a `.corrupt` backup on unreadable/non-dict content before
        returning `{}` — otherwise the very next PATCH/DELETE via the
        schema-driven settings routes would call `cfg.save()` on the empty
        dict and silently overwrite whatever the user had.
        """
        import config as cfg

        if not os.path.exists(cfg.CONFIG_PATH):
            return {}
        try:
            with open(cfg.CONFIG_PATH) as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            cfg._preserve_corrupt_config()
            return {}
        if not isinstance(raw, dict):
            cfg._preserve_corrupt_config()
            return {}
        return raw

    # Serializes read-modify-write of ~/.vireo/config.json and the active
    # workspace's config_overrides across the schema-driven settings
    # endpoints (PATCH/DELETE/import). Without this, with per-field autosave
    # and `app.run(threaded=True)` two concurrent requests can read the same
    # snapshot and the later writer drops the earlier change.
    _settings_write_lock = threading.Lock()
    # Shared by the iNaturalist and settings blueprints so a settings write
    # that changes ``inat_token`` supersedes an in-flight modal validation.
    # Only touched while holding ``_settings_write_lock``.
    _inat_token_generation = InatTokenGeneration()




    # -- Scan status (kept, non-job) --

    # -- Model & Taxonomy API routes --


    # -- Job API routes --

    def _build_scan_work(
        roots, incremental, active_ws, repair_missing_metadata=False,
    ):
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
            from scanner import ScanCancelled
            from scanner import scan as do_scan

            thread_db = Database(db_path)
            thread_db.set_active_workspace(active_ws)
            # Check folder health before scanning to prevent duplicate imports
            if thread_db.check_folder_health():
                _invalidate_missing_originals_cache()

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
            # Photos that actually reached the catalog, summed across roots
            # from each scan()'s counts sink. Kept separate from the
            # progress accumulator above: progress counts every file the
            # scan disposes of (including ones skipped because they
            # vanished under a dropped mount), which is what a progress bar
            # needs but overstates the result line.
            indexed_acc = {"n": 0}

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

            def status_cb(message, phase_current=None, phase_total=None, phase_label=None):
                progress_payload = {
                    "phase": phase_label or message,
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": message,
                    "rate": 0,
                    "phase_current": phase_current,
                    "phase_total": phase_total,
                    "phase_label": phase_label,
                }
                runner.update_step(job["id"], "scan", current_file=message)
                runner.push_event(job["id"], "progress", progress_payload)

            def cancel_check():
                return runner.is_cancelled(job["id"])

            def pause_check():
                return runner.pause_requested(job["id"])

            def cancel_only_check():
                return runner.cancellation_requested(job["id"])

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
            cancelled = False
            for idx, root in enumerate(roots_list, 1):
                if cancel_check():
                    cancelled = True
                    break
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
                # Counts sink rather than the return value: scan() commits
                # rows as it goes and can raise (or be cancelled) after
                # thousands have landed. Reading the sink in `finally`
                # credits that work on every exit path — a root that dies
                # late would otherwise report zero photos indexed.
                root_counts = {}
                try:
                    do_scan(
                        root, thread_db,
                        progress_callback=progress_cb,
                        incremental=incremental,
                        extract_full_metadata=pipeline_cfg.get("extract_full_metadata", True),
                        status_callback=status_cb,
                        vireo_dir=vireo_dir,
                        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                        cancel_check=cancel_check,
                        pause_check=pause_check,
                        cancel_only_check=cancel_only_check,
                        repair_missing_metadata=repair_missing_metadata,
                        counts=root_counts,
                    )
                except Exception as exc:
                    if isinstance(exc, ScanCancelled) and cancel_check():
                        log.info("Scan job %s cancelled during root %s", job["id"], root)
                        cancelled = True
                        break
                    log.exception("Scan failed for root %s", root)
                    scan_failed_roots.add(root)
                    msg = f"[{root}] {exc}"
                    root_errors.append(msg)
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                finally:
                    # Credit whatever this root indexed regardless of how
                    # it exited — completed, raised, or cancelled. The
                    # rows are committed either way, so the summary must
                    # count them either way.
                    indexed_acc["n"] += root_counts.get("indexed", 0)
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
                    # scanner.scan touches disk and may add or remove
                    # photo rows; a ready Missing Originals payload
                    # computed before the scan can now be stale (e.g.
                    # user restored an original before running "Rescan
                    # this Folder"). The pre-scan health-check
                    # invalidation only fires when a folder flips
                    # missing/ok, so also drop the cache once the scan
                    # itself has run — even on partial failure, since
                    # rows are committed incrementally.
                    try:
                        _invalidate_missing_originals_cache()
                    except Exception:
                        log.exception(
                            "Failed to invalidate missing-originals cache for %s",
                            root,
                        )
                    advance_scan_acc()

            if cancelled or cancel_check():
                # Same indexed count as the completed path, not the
                # progress counter. Otherwise "N photos" would mean two
                # different things depending on which branch produced it,
                # and a cancelled scan of a dropped share would report a
                # full house of photos it never cataloged.
                photo_count = indexed_acc["n"]
                runner.update_step(
                    job["id"], "scan", status="cancelled",
                    summary=f"{photo_count} photos (cancelled)",
                )
                runner.update_step(
                    job["id"], "thumbnails", status="skipped",
                    summary="skipped (cancelled)",
                )
                return {"photos_indexed": photo_count, "cancelled": True}

            # Count what the scans reported as indexed, not the progress
            # counter. On a clean run they agree; they diverge exactly when
            # the run went wrong — progress advances for files that were
            # discovered and then skipped (vanished under a mount that
            # dropped mid-scan), so "N photos" would read as a success
            # line for a scan that cataloged nothing.
            photo_count = indexed_acc["n"]
            # Unique roots that hit any failure class. Counting unique
            # roots (not error entries) avoids inflating the "N of M"
            # summary when a single root raises in both scan and cache
            # invalidation.
            failed_root_count = len(scan_failed_roots | cache_failed_roots)
            metadata_warning = _scan_metadata_warning()
            if root_errors:
                scan_summary = (
                    f"{photo_count} photos ({failed_root_count} of "
                    f"{len(roots_list)} root"
                    f"{'s' if len(roots_list) != 1 else ''} failed)"
                )
                if metadata_warning:
                    scan_summary += f" — {metadata_warning}"
                runner.update_step(
                    job["id"], "scan", status="failed", summary=scan_summary,
                    error=root_errors[0], error_count=len(root_errors),
                )
            else:
                scan_summary = f"{photo_count} photos"
                if metadata_warning:
                    scan_summary += f" — {metadata_warning}"
                runner.update_step(
                    job["id"], "scan", status="completed",
                    summary=scan_summary,
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


    def _metadata_repair_count(db, workspace_id, root_paths=None):
        # Don't filter by ``folders.status``: that column is only refreshed
        # by ``check_folder_health`` (10-minute loop or the manual
        # "check missing folders" flow), so a workspace whose drive was
        # unplugged and is now reconnected still reads as ``status='missing'``
        # until then. The readiness endpoint fires as soon as the Import
        # page opens; if we filtered by ``status``, users would see 0
        # repairable photos and the repair route would 409 with "no
        # photos need metadata repair" even though ``os.path.isdir`` would
        # let the repair job scan them.
        #
        # ``root_paths`` scopes the count to real-time reachable roots
        # (based on ``os.path.isdir``). When a workspace mixes an offline
        # drive with photos missing EXIF and a separate reachable drive
        # with no repairable rows, an unscoped count combined with a
        # non-empty ``reachable_roots`` list would enable the Repair
        # button and start a job that finishes without ever touching the
        # offline photos — Codex's "repeating repair job" pathology. The
        # scoped count reflects only what the repair pass would actually
        # process. ``None`` preserves the unscoped legacy shape for any
        # future caller that wants a workspace-wide figure.
        params = [workspace_id]
        where_extra = ""
        if root_paths is not None:
            if not root_paths:
                return 0
            normalized_roots = [
                r.replace("\\", "/").rstrip("/") for r in root_paths if r
            ]
            if not normalized_roots:
                return 0
            clauses = []
            for norm in normalized_roots:
                prefix = norm + "/"
                # Match ``f.path`` normalized to forward slashes either
                # exactly against the root or as a boundary-preserving
                # prefix. ``substr(...)=prefix`` avoids the wildcard
                # collision LIKE would introduce (e.g. a folder called
                # ``photos_backup`` incorrectly matching a reachable
                # ``photos`` root because ``_`` matches any character
                # in LIKE without ESCAPE).
                clauses.append(
                    "(REPLACE(f.path, '\\', '/') = ? "
                    "OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?)"
                )
                params.extend([norm, len(prefix), prefix])
            where_extra = " AND (" + " OR ".join(clauses) + ")"
        rows = db.conn.execute(
            "SELECT DISTINCT p.id, p.filename, "
            "f.id AS folder_id, f.path AS folder_path "
            "FROM photos p "
            "JOIN folders f ON f.id = p.folder_id "
            "JOIN workspace_folders wf ON wf.folder_id = f.id "
            "WHERE wf.workspace_id = ? "
            "AND p.exif_data IS NULL"
            + where_extra
            + " ORDER BY f.path, p.filename",
            params,
        ).fetchall()

        # A database row is only repairable when its original still exists.
        # The incremental repair scan discovers files from disk, so counting
        # a deleted/moved original here would leave the Repair button enabled
        # forever for a job that can never visit that row. Enumerate each
        # candidate folder once instead of statting every photo individually;
        # this keeps readiness responsive for large degraded imports.
        from image_loader import is_excluded_scan_path

        folder_files = {}
        repairable = 0
        for row in rows:
            folder_id = row["folder_id"]
            folder_path = row["folder_path"]
            if folder_id not in folder_files:
                if is_excluded_scan_path(folder_path):
                    folder_files[folder_id] = None
                else:
                    try:
                        with os.scandir(folder_path) as entries:
                            folder_files[folder_id] = {
                                entry.name for entry in entries if entry.is_file()
                            }
                    except OSError:
                        # The folder disappeared or became unreadable after
                        # root reachability was checked. Treat its rows as
                        # unavailable rather than offering a no-op repair.
                        folder_files[folder_id] = None

            names = folder_files[folder_id]
            if names is None:
                continue
            filename = row["filename"]
            if filename in names:
                repairable += 1
                continue
            # Preserve the filesystem's own case and Unicode matching rules
            # for a catalog name that did not compare byte-for-byte with the
            # directory entry (notably default APFS and NTFS volumes).
            if os.path.isfile(os.path.join(folder_path, filename)):
                repairable += 1
        return repairable


    # -- Export presets --

    def _move_folder_guard_error(guard_db, folder_id):
        """Return the error message blocking a folder move, or None.

        Context-free on purpose: takes the db explicitly and touches no
        Flask request/app context, so the chained completion hook can run
        it from a job thread with a thread db.
        """
        # A folder covered by any workspace's local_workspace_folders row has
        # its folders.path rebased into that workspace's managed copy; a
        # concurrent workspace-membership guard would refuse to touch the
        # folders row for exactly this reason. Moving it here (via
        # db.move_folder_path) would move or delete the managed copy and
        # rewrite the catalog while local_workspace_folders and the manifest
        # still expect the pre-move layout, so the owning workspace's next
        # status falls into missing-local recovery and sync/discard can no
        # longer restore. Reject the job before enqueue.
        with stage_boundary_lock():
            local_root_id = local_root_for_folder(guard_db, folder_id)
            if local_root_id is not None:
                return (
                    "Cannot move this folder while it has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            # A descendant local copy has already had its folders.path rebased
            # under local-folders/, so a folders.path subtree walk from this
            # ancestor no longer sees it — but local_folder_mappings.source_path
            # still records the original location. Without this check the move
            # job would move/delete the original source directory out from
            # under the manifest, leaving sync/discard unable to restore.
            descendant_root_id = local_root_under_folder(guard_db, folder_id)
            if descendant_root_id is not None:
                return (
                    "Cannot move this folder while a subfolder has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            staged_here, staged_owner_ws = folder_has_local_workspace(
                guard_db, folder_id,
            )
            if staged_here:
                return (
                    f"Cannot move this folder — workspace {staged_owner_ws} has it "
                    "staged locally. Switch to that workspace and sync or discard the "
                    "local copy first."
                )
            # ``folder_has_local_workspace`` only sees a completed stage
            # claim. A stage/sync/discard queued or running against a
            # workspace that contains this folder hasn't yet written
            # local_workspace_folders, so the read-only guard above passes
            # even though the transition worker is about to rebase this
            # folder's paths. Enqueueing the move now would rewrite the
            # ``folders`` row out from under the pending transition —
            # missing-local recovery on the next status. Refuse until the
            # transition finishes.
            row = guard_db.conn.execute(
                "SELECT workspace_id FROM workspace_folders WHERE folder_id = ?",
                (folder_id,),
            ).fetchall()
            for ws_row in row:
                pending = _pending_local_workspace_transition(
                    int(ws_row["workspace_id"]), db=guard_db)
                if pending:
                    return (
                        f"Wait for the {pending['type']} job on workspace "
                        f"{int(ws_row['workspace_id'])} to finish before moving this "
                        "folder; otherwise the move would run on paths that workspace "
                        "is about to claim."
                    )
        return None

    def _start_move_folder_job(runner, workspace_id, *, folder_id,
                               destination, display_dest, destination_name,
                               source_path, resolved_destination,
                               merge, remote, developed_dir, folder_template="",
                               date_destinations=None,
                               chained_from=None, serialize_lock=None,
                               allow_tracked_merge=False,
                               managed_staging_root=None, mount_baseline=None,
                               mount_identities=None):
        """Enqueue a move-folder job and return its job id.

        Shared by the move-folder endpoint and the chained
        process-completion hook (which runs on a job thread, so this
        must not touch Flask request/app context).

        ``serialize_lock``: optional ``threading.Lock`` shared by a batch
        of chained moves; when given, the transfer itself runs under the
        lock so batch-mates execute one at a time (see the why-comment at
        the acquire site). The job still enqueues — and its id exists —
        immediately.

        ``allow_tracked_merge``: passed through to ``move_folder``. The
        chained import→process→move hook opts in (see the why-comment in
        ``_enqueue_move_folder_job``); the manual move endpoint keeps the
        default refusal of tracked destinations.

        ``date_destinations``: the planned capture-date folders for a
        date-organized move (``path``/``relative_path``/``photo_count`` per
        entry, as produced by ``plan_folder_date_moves``). Snapshotted into
        the job config so the jobs panel can name the folders photos actually
        land in rather than only the selected root.
        """
        def work(job):
            from move import move_folder, move_folder_by_date

            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)

            job["_start_time"] = time.time()

            last_phase = {"value": None}

            def progress_cb(current, total, filename, phase="Moving folder"):
                # Only update keys JobRunner pre-seeds in job["progress"]
                # (current/total/current_file). Do NOT insert "phase" here:
                # this runs on the worker thread outside the runner lock, and
                # adding a new key races _snapshot_job's locked dict() copy
                # ("dictionary changed size during iteration"). push_event
                # below mirrors phase onto job["progress"] under the lock.
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                # The copy phase fires once per file; on a large folder that
                # would flood the SSE stream and tie up Flask threads. Throttle
                # to every 10th file, but always emit on a phase change and on
                # the first/last file so the panel never looks stalled.
                phase_changed = phase != last_phase["value"]
                last_phase["value"] = phase
                if not phase_changed and current % 10 != 0 \
                        and current not in (1, total):
                    return
                runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "phase": phase,
                })

            # The chained moves from one import all rsync to the same NAS,
            # and runner.start gives each its own thread immediately — so
            # N folders would mean N concurrent rsyncs, each honoring
            # --bwlimit individually and together consuming N× the
            # configured bandwidth budget. The chain hands every job in
            # the batch one shared lock so the transfers run one at a
            # time while all N jobs (and their ids) still enqueue up
            # front.
            if serialize_lock is not None and not serialize_lock.acquire(blocking=False):
                # UI transparency: a job blocked on the batch lock
                # must say why it isn't moving anything yet.
                runner.push_event(job["id"], "progress", {
                    "current": 0,
                    "total": 0,
                    "current_file": "",
                    "phase": (
                        "Waiting for an earlier chained move to finish"
                    ),
                })
                # Poll instead of blocking outright: this wait is the
                # one boundary where Cancel can be honored without
                # touching mid-transfer semantics — a blocking acquire
                # would run the whole transfer anyway after the user
                # pressed Stop.
                while not serialize_lock.acquire(timeout=0.5):
                    if runner.is_cancelled(job["id"]):
                        # Returning with the lock NOT held, before the
                        # try below — so the release in its finally
                        # never fires on this path.
                        return {
                            "ok": False, "moved": 0, "errors": [],
                            "summary": (
                                "Cancelled before transfer started"
                            ),
                        }
            # Cancel check with the lock held, covering two paths:
            # (1) the waiter's ``acquire(timeout=0.5)`` returned True in
            # the same 0.5s window a cancel landed in, exiting the loop
            # without the in-loop check running; (2) a chained move whose
            # thread started AFTER the earlier holder already released
            # the batch lock — its non-blocking acquire succeeds so it
            # never enters the wait loop, but its ``/cancel`` may have
            # already been accepted while the thread was still queued.
            # Either way, don't start the transfer.
            if serialize_lock is not None and runner.is_cancelled(job["id"]):
                serialize_lock.release()
                return {
                    "ok": False, "moved": 0, "errors": [],
                    "summary": (
                        "Cancelled before transfer started"
                    ),
                }
            try:
                check_mount = None
                if managed_staging_root and not runner.begin_uncancellable(job["id"]):
                    return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
                if mount_baseline is not None:
                    from import_staging import check_staged_mount

                    def check_mount():
                        check_staged_mount(resolved_destination, mount_baseline, mount_identities)
                    check_mount()
                if folder_template:
                    result = move_folder_by_date(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        folder_template=folder_template,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                    )
                else:
                    result = move_folder(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                        merge=merge,
                        remote=remote,
                        destination_name=destination_name,
                        allow_tracked_merge=allow_tracked_merge,
                        thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                        **({"verify_contents": True} if managed_staging_root and not remote else {}),
                        **({"pre_commit_check": check_mount} if check_mount else {}),
                    )
            finally:
                if serialize_lock is not None:
                    serialize_lock.release()

            # Tell the JobRunner whether the move actually succeeded. Without
            # this the runner marks any normal return "completed" — so a move
            # that copied nothing because rsync timed out used to read as
            # "completed, 0 errors" in the history. A `needs_merge` return is
            # NOT a failure: it's a soft signal that the destination already
            # exists and the UI should re-prompt for a merge/resume, so leave
            # it for the caller without flagging the job failed.
            if not result.get("needs_merge"):
                errors = result.get("errors") or []
                moved = result.get("moved", 0)
                if errors and moved == 0:
                    result["ok"] = False
                    result["summary"] = f"Move failed — {errors[0]}"
                else:
                    cleanup_error = result.get("cleanup_error")
                    result["ok"] = True
                    result["summary"] = (
                        f"Moved {moved} photo{'s' if moved != 1 else ''}"
                        + (f", {len(errors)} error(s)" if errors else "")
                        + (
                            f"; cleanup failed: {cleanup_error}"
                            if cleanup_error else ""
                        )
                    )
            if result.get("ok"):
                if managed_staging_root:
                    from path_guard import contains_resolved
                    # Remove only empty staging ancestors. Failed transfers and
                    # concurrent sibling moves keep their originals intact.
                    parent = os.path.dirname(source_path)
                    root = os.path.realpath(managed_staging_root)
                    while contains_resolved(root, parent):
                        try:
                            os.rmdir(parent)
                        except OSError:
                            break
                        if os.path.realpath(parent) == root:
                            break
                        parent = os.path.dirname(parent)
                try:
                    _invalidate_missing_originals_cache()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after move-folder job",
                    )
            return result

        job_config = {
            "folder_id": folder_id, "destination": display_dest, "merge": merge,
            # Snapshot both ends of the move when it is enqueued. The source
            # catalog row is rewritten after a successful move, so resolving
            # it later would make completed jobs misleading. Likewise,
            # ``destination`` above is only the selected parent; the jobs UI
            # needs the actual landing path (including a rename/source leaf).
            "source_path": source_path,
            "resolved_destination": resolved_destination,
        }
        if folder_template:
            job_config["folder_template"] = folder_template
        if date_destinations:
            # Cap the stored list: a multi-year source folder can plan
            # thousands of date folders, and the whole config is serialized
            # into the job row and every status poll. The panel shows the
            # first few and reports the true totals from the counts below,
            # which are computed over the full plan.
            job_config["date_destinations"] = [
                {
                    "path": item["path"],
                    "relative_path": item["relative_path"],
                    "photo_count": item["photo_count"],
                }
                for item in date_destinations[:MOVE_DATE_DEST_PREVIEW_LIMIT]
            ]
            job_config["date_destination_count"] = len(date_destinations)
            job_config["date_photo_count"] = sum(
                item["photo_count"] for item in date_destinations)
        if destination_name:
            job_config["destination_name"] = destination_name
        if remote:
            # Surface that this is an SSH transfer (and to where) so the job
            # panel can show it, per the UI-transparency rule.
            job_config["remote"] = {
                "host": remote["host"], "user": remote["user"],
                "ssh_dest_base": remote["ssh_dest_base"],
                "mount_dest_base": remote["mount_dest_base"],
                "bwlimit_kbps": remote["bwlimit_kbps"],
            }
        if chained_from:
            # Provenance for the jobs panel: this move was started by a
            # chained process run's completion hook, not by hand.
            job_config["chained_from"] = chained_from

        def staged_work(job):
            runner.push_event(job["id"], "progress", {
                "current": 0, "total": 0, "current_file": "",
                "phase": "Waiting for workspace jobs to finish before sending to NAS",
            })
            if not runner.wait_for_workspace_transfer(job["id"]):
                return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
            return work(job)

        return runner.start(
            "move-folder", staged_work if managed_staging_root else work,
            config=job_config,
            workspace_id=workspace_id,
            **({"workspace_transfer_batch": managed_staging_root} if managed_staging_root else {}),
        )





    # -- Image serving --



    # -- Pipeline: SAM2 Mask Extraction --


    def _enqueue_move_folder_job(thread_db, runner, workspace_id, *,
                                 folder_id, subpath, target,
                                 chained_from=None, serialize_lock=None):
        """Enqueue a chained remote move for one imported folder.

        Job-thread path into move-folder (no request context). ``target`` is
        the snapshot captured when the import was enqueued — deliberately NOT
        re-resolved from Settings here, so a mid-chain edit cannot redirect
        the move. Raises on any precondition failure — the caller records the
        failure per folder rather than aborting the batch.
        """
        import posixpath

        import config as cfg
        import move as move_mod

        mount_path = (target.get("mount_path") or "").strip()
        if not mount_path:
            raise RuntimeError(
                "remote target has no local mount path, so moved photos "
                "couldn't stay in your library — add one under Settings → "
                "Remote targets")
        if not os.path.isabs(mount_path):
            raise RuntimeError(
                f"remote target's local mount path isn't absolute "
                f"(\"{mount_path}\") — fix it under Settings → Remote targets")
        guard = _move_folder_guard_error(thread_db, folder_id)
        if guard:
            raise RuntimeError(guard)
        effective_cfg = thread_db.get_effective_config(cfg.load())
        if target.get("transport") == "mounted":
            folder = thread_db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (folder_id,),
            ).fetchone()
            if not folder:
                raise RuntimeError("folder no longer exists")
            destination = os.path.join(mount_path, *posixpath.dirname(subpath).split("/"))
            return _start_move_folder_job(
                runner, workspace_id, folder_id=folder_id,
                destination=destination, display_dest=destination,
                destination_name="", source_path=folder["path"],
                resolved_destination=os.path.join(destination, os.path.basename(folder["path"])),
                merge=True, remote=None,
                developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
                chained_from=chained_from, serialize_lock=serialize_lock,
                allow_tracked_merge=True,
                managed_staging_root=target.get("managed_staging_root"),
                mount_baseline=target.get("mount_baseline"),
                mount_identities=target.get("mount_identities"),
            )
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        if not rsync_bin:
            raise RuntimeError("no usable GNU rsync for remote moves")
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        if not ssh_bin:
            raise RuntimeError("OpenSSH client not found")
        # ``move_folder`` lands the source folder INSIDE the destination,
        # keeping the folder's own name — so to mirror the archive layout
        # (``<local_archive_root>/2026/trip`` → ``<remote_path>/2026/trip``)
        # the spec's subpath must be the folder's PARENT ("2026"), not the
        # full archive-relative subpath, or the leaf would double up
        # ("2026/trip/trip").
        remote = move_mod.build_remote_move_spec(
            target, posixpath.dirname(subpath), rsync_bin, ssh_bin)
        folder = thread_db.conn.execute(
            "SELECT path, name FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not folder:
            raise RuntimeError("folder no longer exists")
        landing_name = folder["name"] \
            or os.path.basename(folder["path"].rstrip("/\\"))
        resolved_destination = move_mod.rsync_dest_spec(
            target,
            posixpath.join(remote["ssh_dest_base"], landing_name),
        )
        return _start_move_folder_job(
            runner, workspace_id,
            folder_id=folder_id,
            destination=remote["mount_dest_base"],
            display_dest=move_mod.rsync_dest_spec(
                target, remote["ssh_dest_base"]),
            destination_name="",
            source_path=folder["path"],
            resolved_destination=resolved_destination,
            merge=True,
            remote=remote,
            developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
            chained_from=chained_from,
            serialize_lock=serialize_lock,
            # Re-importing more photos into an existing shoot folder is the
            # NORMAL flow, and its chained move lands exactly on the tracked
            # NAS copy created by the previous chain run. Without tracked-
            # merge the move would refuse ("Destination overlaps a folder
            # Vireo already manages") and strand the new photos locally.
            # Opting in uses move_folder's exact-overlap reconciliation
            # (fold the new rows into the existing archive rows); the
            # pre-copy content-conflict scan still refuses any same-name
            # file whose bytes differ, and manual moves keep the default
            # refusal.
            allow_tracked_merge=True,
            managed_staging_root=target.get("managed_staging_root"),
        )



    app.register_blueprint(
        create_media_blueprint(
            _get_db,
            json_error,
            db_path,
            app.config,
            invalid_preview_cache_paths=_invalid_preview_cache_paths,
            clear_preview_cache_invalid=_clear_preview_cache_invalid,
            photo_not_found_error=_photo_not_found_error,
        )
    )
    # The prepare-full-resolution job calls the /original view directly so
    # its RAW/companion/edit fallbacks cannot drift from the lightbox's.
    serve_original_photo = app.view_functions["media.serve_original_photo"]

    app.register_blueprint(
        create_photo_labels_blueprint(
            _get_db, json_error, settings_write_lock=_settings_write_lock
        )
    )
    app.register_blueprint(create_photo_review_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_life_list_blueprint(
            _get_db,
            json_error,
            build_life_list_payload=_build_life_list_payload,
        )
    )
    app.register_blueprint(
        create_audit_blueprint(
            _get_db,
            json_error,
            app.config,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_settings_blueprint(
            _get_db,
            json_error,
            app.config,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            advance_inat_token_generation=_inat_token_generation.advance,
        )
    )
    app.register_blueprint(
        create_inat_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            token_generation=_inat_token_generation,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
            max_selection_photos=MAX_SELECTION_PHOTOS,
        )
    )
    app.register_blueprint(
        create_storage_blueprint(
            _get_db, json_error, db_path, app.config, chunked=_chunked,
        )
    )
    app.register_blueprint(
        create_workspace_blueprint(
            _get_db,
            json_error,
            ALL_PAGES,
            get_runner=lambda: app._job_runner,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            settings_write_lock=_settings_write_lock,
            new_images_walk_progress=app._new_images_walk_progress,
            missing_originals_heavy_job_types=_MISSING_ORIGINALS_HEAVY_JOB_TYPES,
        )
    )
    app.register_blueprint(
        create_folders_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            build_scan_work=_build_scan_work,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(create_capture_time_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_remote_setup_blueprint(_get_db, json_error, app.config)
    )
    app.register_blueprint(
        create_editing_blueprint(
            _get_db,
            json_error,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
        )
    )
    app.register_blueprint(create_species_blueprint(_get_db))
    app.register_blueprint(
        create_system_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            get_log_broadcaster=lambda: app._log_broadcaster,
            settings_write_lock=_settings_write_lock,
            read_raw_config_file=_read_raw_config_file,
        )
    )
    app.register_blueprint(
        create_misses_blueprint(
            _get_db,
            json_error,
            settings_write_lock=_settings_write_lock,
            resolve_visual=visual_scope.resolve,
        )
    )
    app.register_blueprint(
        create_browse_blueprint(
            _get_db,
            json_error,
            visual_scope=visual_scope,
            max_per_page=_MAX_PER_PAGE,
            ambiguous_prediction_ids=_ambiguous_prediction_ids,
        )
    )
    app.register_blueprint(
        create_predictions_blueprint(
            _get_db,
            json_error,
            app.config,
            visual_scope=visual_scope,
            chunked=_chunked,
            ambiguous_prediction_ids=_ambiguous_prediction_ids,
            effective_category_resolver=_effective_category_resolver,
            prediction_is_ambiguous=_prediction_is_ambiguous,
        )
    )
    app.register_blueprint(
        create_encounters_blueprint(
            _get_db, json_error, db_path, chunked=_chunked,
        )
    )
    # Registered before the /api/v1 alias loop below, which aliases two of
    # these endpoints by their ``collections.``-qualified names.
    app.register_blueprint(
        create_collections_blueprint(
            _get_db, json_error, max_per_page=_MAX_PER_PAGE,
        )
    )
    app.register_blueprint(create_dashboard_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_sync_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
        )
    )
    from web.move_cleanup import create_move_cleanup_blueprint
    app.register_blueprint(create_move_cleanup_blueprint(
        _get_db, lambda: app._job_runner, json_error,
        lambda paths: _trash_paths(paths), _move_folder_guard_error,
    ))
    app.register_blueprint(create_moves_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_duplicates_blueprint(
            _get_db,
            json_error,
            chunked=_chunked,
            # Late-bound through the module globals, like the move-cleanup
            # blueprint's trash hook above, so a patched ``app._trash_paths``
            # or ``app._network_volume_roots`` still reaches these routes.
            trash_paths=lambda *args, **kwargs: _trash_paths(*args, **kwargs),
            network_volume_roots=lambda: _network_volume_roots(),
            path_on_network_volume=_path_on_network_volume,
            cleanup_cached_files_for_deleted_photos=(
                _cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    app.register_blueprint(
        create_card_cleanup_blueprint(
            _get_db, json_error, lambda: app._job_runner, db_path, app.config,
        )
    )
    app.register_blueprint(
        create_models_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            count_keywords=init_db.count_keywords,
        )
    )
    app.register_blueprint(
        create_caches_blueprint(_get_db, json_error, db_path, app.config)
    )
    app.register_blueprint(
        create_export_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            build_life_list_payload=_build_life_list_payload,
            build_highlights_payload=_build_highlights_payload,
            resolve_visual=visual_scope.resolve,
        )
    )
    app.register_blueprint(
        create_pipeline_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
        )
    )
    # Built here rather than at the top: the after-process NAS move hands
    # off to _enqueue_move_folder_job, which is defined late in create_app
    # and passed by value.
    pipeline_chain = PipelineChain(
        get_runner=lambda: app._job_runner,
        db_path=db_path,
        config=app.config,
        invalidate_missing_originals=_invalidate_missing_originals_cache,
        enqueue_move_folder_job=_enqueue_move_folder_job,
    )
    app.register_blueprint(
        create_imports_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            metadata_repair_count=_metadata_repair_count,
            enqueue_process_job=pipeline_chain.enqueue_process_job,
            chain_after_move=pipeline_chain.chain_after_move,
            bulk_gps_location_payload=_bulk_gps_location_payload,
            guard_move_folder=_move_folder_guard_error,
            sync_job_lock=app._sync_job_lock,
        )
    )
    app.register_blueprint(create_keywords_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_locations_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            normalize_photo_id_list=_normalize_photo_id_list,
            bulk_gps_location_source_ids=_bulk_gps_location_source_ids,
            location_keyword_photo_ids=_location_keyword_photo_ids,
            google_reverse_geocode=_google_reverse_geocode,
            decode_cached_reverse_geocode=_decode_cached_reverse_geocode,
            encode_cached_reverse_geocode=_encode_cached_reverse_geocode,
            summarize_details=_summarize_details,
        )
    )
    app.register_blueprint(
        create_batch_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            bulk_gps_location_payload=_bulk_gps_location_payload,
            run_batch_delete=_run_batch_delete,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
        )
    )
    # Registered before the /api/v1 alias loop below, which aliases
    # ``photos.api_photos`` and ``photos.api_photo_detail``.
    app.register_blueprint(
        create_photos_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            visual_scope=visual_scope,
            photo_not_found_error=_photo_not_found_error,
            max_per_page=_MAX_PER_PAGE,
            request_flag_filter=_request_flag_filter,
            request_location_status_filter=_request_location_status_filter,
            parse_missing_originals_folder_id=_parse_missing_originals_folder_id,
            missing_originals_payload=_missing_originals_payload,
            start_missing_originals_scan=_start_missing_originals_scan,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            run_batch_delete=_run_batch_delete,
            photo_highlight_entries=photo_highlight_entries,
            best_batch_scope=best_batch_scope,
            build_best_batch_response=build_best_batch_response,
        )
    )
    app.register_blueprint(
        create_photo_edit_recipes_blueprint(
            _get_db,
            json_error,
            app.config,
            photo_not_found_error=_photo_not_found_error,
            invalidate_photo_render_cache=_invalidate_photo_render_cache,
            queue_edit_recipe_sync=_queue_edit_recipe_sync,
        )
    )
    app.register_blueprint(
        create_history_blueprint(
            _get_db,
            json_error,
            db_path,
            invalidate_photo_render_cache=_invalidate_photo_render_cache,
            queue_edit_recipe_sync=_queue_edit_recipe_sync,
        )
    )
    app.register_blueprint(
        create_photo_location_keywords_blueprint(
            _get_db,
            json_error,
            photo_not_found_error=_photo_not_found_error,
            location_errors=location_errors,
        )
    )

    app.register_blueprint(
        create_job_launchers_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            chunked=_chunked,
            invalidate_missing_originals=_invalidate_missing_originals_cache,
            run_batch_delete=_run_batch_delete,
            build_scan_work=_build_scan_work,
            pending_local_workspace_transition=_pending_local_workspace_transition,
            read_raw_config_file=_read_raw_config_file,
            settings_write_lock=_settings_write_lock,
            metadata_repair_count=_metadata_repair_count,
            guard_move_folder=_move_folder_guard_error,
            start_move_folder_job=_start_move_folder_job,
            # Late-bound so it resolves whichever ``serve_original_photo``
            # create_app holds when a job runs, not when the app is built.
            serve_original_photo=lambda *args, **kwargs: serve_original_photo(
                *args, **kwargs
            ),
            sync_job_lock=app._sync_job_lock,
        )
    )

    # --- /api/v1/* aliases over the stable subset of /api/* ---
    # These are the endpoints advertised to external callers in docs/headless-api.md.
    # Keep this list tight — expanding it locks the surface.
    _V1_ALIASES = [
        # (v1 path, existing endpoint name, methods)
        ("/api/v1/photos", "photos.api_photos", ["GET"]),
        ("/api/v1/photos/<int:photo_id>", "photos.api_photo_detail", ["GET"]),
        ("/api/v1/collections", "collections.api_collections", ["GET"]),
        ("/api/v1/collections/<int:collection_id>/photos",
         "collections.api_collection_photos", ["GET"]),
        ("/api/v1/workspaces", "workspaces.api_get_workspaces", ["GET"]),
        ("/api/v1/workspaces/<int:ws_id>/activate",
         "workspaces.api_activate_workspace", ["POST"]),
        ("/api/v1/keywords", "keywords.api_keywords", ["GET"]),
    ]

    for v1_path, endpoint_name, methods in _V1_ALIASES:
        view = app.view_functions.get(endpoint_name)
        if view is None:
            raise RuntimeError(
                f"Cannot alias {v1_path}: endpoint '{endpoint_name}' not registered"
            )
        # Blueprint endpoints are aliased under their bare view name so the
        # v1 endpoint names stay ``v1_<view>`` whichever module owns the route.
        v1_endpoint = f"v1_{endpoint_name.rpartition('.')[2]}"
        app.add_url_rule(
            v1_path,
            endpoint=v1_endpoint,
            view_func=view,
            methods=methods,
        )
        # An alias is exempt from the workspace mutation reservation exactly
        # when the view it aliases is, so the two surfaces can't drift.
        if endpoint_name in _reservation_exempt_endpoints:
            _reservation_exempt_endpoints.add(v1_endpoint)

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        # Give the main thread enough time to return from create_app and bind
        # the HTTP listener before this potentially multi-minute XMP scan
        # starts competing for filesystem and interpreter time.
        _wildlife_retirement_timer = threading.Timer(
            1.0, _retire_wildlife_genre,
        )
        _wildlife_retirement_timer.daemon = True
        _wildlife_retirement_timer.start()

    return app


def _emit_incompatible_database_exit(e):
    # Both --load-taxonomy and create_app open the catalog; either can trip
    # ensure_schema's newer-DB guard, and both need the same guided exit so
    # the desktop launcher gets a structured signal instead of a raw
    # traceback.
    import sys as _sys
    if getattr(e, "newer", False):
        log.error(
            "Cannot open database at %s: it was created by a newer "
            "version of Vireo than this build supports. Update Vireo to "
            "its latest version to open this catalog. Underlying error: %s",
            e.db_path, e.cause,
        )
    else:
        log.error(
            "Cannot open database at %s: it is from an incompatible older "
            "version of Vireo. Back it up and remove it to start fresh "
            "(e.g. `mv %s %s.bak`), then relaunch. Underlying error: %s",
            e.db_path, e.db_path, e.db_path, e.cause,
        )
    _sys.stderr.write(json.dumps({
        "error": "incompatible_database",
        "db_path": e.db_path,
        "reason": str(e),
        "newer": getattr(e, "newer", False),
    }) + "\n")
    raise SystemExit(3) from e


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
    parser.add_argument(
        "--check-exiftool",
        action="store_true",
        help="Verify the bundled/system ExifTool, print its version, and exit",
    )
    args = parser.parse_args()

    if args.check_exiftool:
        from metadata import exiftool_status

        status = exiftool_status()
        if status["available"]:
            print(status["version"] or "unknown")
            raise SystemExit(0)
        print(status.get("error") or status.get("hint") or "ExifTool unavailable", file=sys.stderr)
        raise SystemExit(1)

    if args.headless:
        args.no_browser = True

    if args.load_taxonomy:
        from db import Database
        from taxonomy import fetch_common_names, load_taxonomy, seed_informal_groups
        # Run the newer-schema guard before Database(args.db) executes any
        # legacy DDL/ALTERs against a catalog stamped by a future Vireo build.
        # create_app takes this same check through ensure_schema; keep the two
        # entry points in sync so `--load-taxonomy` can't corrupt a newer DB.
        try:
            ensure_schema(args.db)
        except IncompatibleDatabaseError as e:
            _emit_incompatible_database_exit(e)
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

    try:
        app = create_app(
            db_path=args.db, thumb_cache_dir=args.thumb_dir, api_token=api_token,
        )
    except IncompatibleDatabaseError as e:
        # The database file predates a schema change this build can't migrate.
        # Fail fast with actionable guidance instead of letting a raw
        # OperationalError traceback escape (which the sidecar host only sees
        # as "did not become healthy within 30s"). The atexit/SIGTERM cleanup
        # registered above releases the single-instance lock and runtime.json
        # on this exit, so a retry isn't blocked by a stale reservation.
        _emit_incompatible_database_exit(e)
    except Exception as e:
        # Any other failure to build the app is still a fatal startup error
        # (corrupt-but-not-stale DB, missing/locked resource, an unexpected
        # bug, ...). Emit the same structured signal the lock-fault path uses
        # so the desktop launcher can surface an actionable dialog instead of
        # leaving the user with a blank window or a generic 30s health-check
        # timeout. The full traceback still goes to the log for diagnosis.
        import sys as _sys
        log.exception("Vireo failed to start while initializing the app")
        _sys.stderr.write(json.dumps({
            "error": "startup_failed",
            "reason": str(e) or e.__class__.__name__,
        }) + "\n")
        raise SystemExit(2) from e

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

    # Waitress uses a fixed thread pool (default 4). Each open SSE stream
    # (bottom-panel logs, job progress, import duplicate check) pins one
    # thread for its whole lifetime, so the pool must be sized well above
    # the plausible number of concurrent streams or page loads queue
    # behind them and the app appears frozen.
    from waitress import serve as waitress_serve
    waitress_serve(app, host="127.0.0.1", port=port, threads=16)


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
    # --pty-spawn-helper: pty setup shim dispatched by remote_setup's
    # install-key path. In the packaged (PyInstaller --onefile) build
    # sys.executable is this same binary, so remote_setup can't shell
    # out via `[python, "-c", helper]` and instead re-executes us with
    # this flag. Must run BEFORE argparse — the wrapped ssh argv follows
    # and would otherwise get rejected as unknown options.
    if len(sys.argv) >= 3 and sys.argv[1] == "--pty-spawn-helper":
        import fcntl
        import termios
        os.setsid()
        fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        a = termios.tcgetattr(0)
        a[3] &= ~(termios.ECHO | termios.ECHOE | termios.ECHOK
                  | termios.ECHONL)
        termios.tcsetattr(0, termios.TCSANOW, a)
        os.execvp(sys.argv[2], sys.argv[2:])
    main()
