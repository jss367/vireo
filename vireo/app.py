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

import id_conflicts
import remote_setup
from db import (
    Database,
    IncompatibleDatabaseError,
)
from flask import (
    Flask,
)
from jobs import JobRunner, LogBroadcaster
from preview_cache import (
    reconcile_preview_cache,
)
from proc import no_window_kwargs
from schema import ensure_schema
from services import scan_work, startup_tasks
from services.folder_moves import FolderMoves
from services.gps_locations import BulkGpsLocations
from services.missing_originals import MissingOriginals
from services.photo_deletion import PhotoDeletion
from services.pipeline_launch import PipelineChain
from services.render_cache import RenderCache
from services.visual_scope import (
    VisualScope,
)
from sql_chunks import chunked as _chunked
from volume_reachability import (  # noqa: F401  (re-exported for tests)
    _NETWORK_PROBE_LOCK,
    _NETWORK_PROBES,
)
from volume_reachability import (
    network_root_reachable as _network_root_reachable,
)
from web import app_hooks
from web.audit import create_audit_blueprint
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
from web.local_folder import create_local_folder_blueprint
from web.local_workspace import create_local_workspace_blueprint
from web.location_edits import LocationErrors
from web.locations import create_locations_blueprint
from web.media import create_media_blueprint
from web.misses import create_misses_blueprint
from web.models import create_models_blueprint
from web.move_cleanup import create_move_cleanup_blueprint
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
from web.responses import json_error, photo_not_found_error
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

    # Request hooks live in web.app_hooks; ``_get_db`` is the per-request
    # catalog connection every blueprint factory receives.
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

    # Location error responses shared by the location, place, and batch
    # routes (``web.location_edits``); built once around ``json_error``.
    location_errors = LocationErrors(
        json_error=json_error, photo_not_found_error=photo_not_found_error,
    )

    # Render/preview cache invalidation (services.render_cache).
    render_cache = RenderCache(app.config)

    # Batch delete and the post-delete cache sweep (services.photo_deletion).
    # The filesystem helpers are wrapped in lambdas so they are looked up on
    # this module at call time — tests monkeypatch ``app._trash_paths`` /
    # ``app._chunked`` and must keep reaching the delete path.
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
    # Startup/maintenance passes; the per-app instance caches the startup
    # taxonomy parse so overlapping one-time migrations and the immediate
    # background species pass do not each parse taxonomy.json.
    startup = startup_tasks.StartupTasks(app, db_path, init_db)
    # Ordered one-shot catalog repairs and config migrations. They run before
    # the first request so no page renders a row they are about to change.
    startup.run_catalog_repairs()

    # Keep taxonomy typing and duplicate-species repair fresh in the
    # background. Wildlife classification eligibility is stored separately
    # on photos; species marking no longer materializes a Wildlife keyword.
    import threading

    # Tests and one-shot tools can invoke the pass deterministically without
    # enabling production timers. Production schedules it only after every
    # route has been registered, immediately before create_app returns.
    app._retire_wildlife_genre = startup.retire_wildlife_genre

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        threading.Thread(target=startup.mark_species, daemon=True).start()

    # Missing Originals scan cache and the folder-health loop. The app
    # attributes below are the service's own lock and dicts (not copies), so
    # code and tests that reach ``app._missing_originals_*`` see live state.
    # Built before FolderMoves, build_scan_work and PipelineChain, which
    # capture ``missing_originals.invalidate`` at construction.
    missing_originals = MissingOriginals(
        db_path=db_path,
        config=app.config,
        get_runner=lambda: app._job_runner,
    )
    app._missing_originals_lock = missing_originals.lock
    app._missing_originals_cache = missing_originals.cache
    app._missing_originals_inflight = missing_originals.inflight
    app._missing_originals_errors = missing_originals.errors
    app._missing_originals_generation = missing_originals.generation

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
        threading.Thread(target=missing_originals.folder_health_loop, daemon=True).start()

    app._job_runner = JobRunner(db=init_db)

    # XMP sidecars are read-modify-written files; serialize sync jobs so
    # repeated clicks cannot race while touching the same sidecar.
    app._sync_job_lock = threading.Lock()
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    app._cleanup_app_resources = startup.cleanup_app_resources

    # Live progress of the most recent new-images walk, keyed by
    # (db_path, workspace_id). Written by the walk's progress callback and
    # read by the GET/POST endpoints so a ``pending`` response can say
    # "38,000 files checked, 2,100 new so far" instead of a bare spinner —
    # the transparency the banner-click path needs on multi-minute walks
    # over large network volumes. Values are per-spawn dicts; a new walk
    # replaces the key wholesale, so readers never see torn state.
    app._new_images_walk_progress = {}

    # Working copies are generated by imports, scans, and on-demand reads.
    # Do not warm the library-wide cache at startup: it consumes disk and
    # CPU for photos the user has not requested.

    # thumb_path self-healing backfill: aligns photos.thumb_path with the
    # thumbnails on disk (see StartupTasks.kickoff_thumb_path_backfill).
    # Ephemeral JobRunner job, skipped entirely when a fast count check finds
    # nothing to do.
    app._kickoff_thumb_path_backfill = startup.kickoff_thumb_path_backfill

    if not os.environ.get("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS"):
        _thumb_backfill_timer = threading.Timer(6.0, startup.kickoff_thumb_path_backfill)
        _thumb_backfill_timer.daemon = True
        _thumb_backfill_timer.start()

    # -- Per-app services shared by several blueprints --

    # Resolves visual-search clauses; owns the per-app query-text
    # embedding cache, so every route shares one instance.
    visual_scope = VisualScope()

    # Bulk EXIF-GPS location flows (services.gps_locations), bound to
    # ``json_error`` and ``location_errors``.
    bulk_gps_locations = BulkGpsLocations(
        json_error=json_error, location_errors=location_errors,
    )

    # Shared by the iNaturalist and settings blueprints so a settings write
    # that changes ``inat_token`` supersedes an in-flight modal validation.
    # Only touched while holding ``config.settings_write_lock``.
    inat_token_generation = InatTokenGeneration()

    # Scan work and folder moves (services.scan_work, services.folder_moves).
    build_scan_work = functools.partial(
        scan_work.build_scan_work,
        get_runner=lambda: app._job_runner,
        db_path=db_path,
        config=app.config,
        invalidate_missing_originals=missing_originals.invalidate,
    )
    folder_moves = FolderMoves(
        get_runner=lambda: app._job_runner,
        get_db=_get_db,
        db_path=db_path,
        config=app.config,
        invalidate_missing_originals=missing_originals.invalidate,
    )

    # The import→process→move chain; the after-process NAS move hands off
    # to ``folder_moves.enqueue_job``.
    pipeline_chain = PipelineChain(
        get_runner=lambda: app._job_runner,
        db_path=db_path,
        config=app.config,
        invalidate_missing_originals=missing_originals.invalidate,
        enqueue_move_folder_job=folder_moves.enqueue_job,
    )

    # -- Blueprints --
    # Per-app service methods are passed as bound methods at registration;
    # pure helpers are imported by the blueprint modules themselves.

    app.register_blueprint(create_pages_blueprint(_get_db))
    app.register_blueprint(create_highlights_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_local_workspace_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            os.path.dirname(app.config["THUMB_CACHE_DIR"]),
            invalidate_missing_originals=lambda ws_id: missing_originals.invalidate(
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
            invalidate_missing_originals=missing_originals.invalidate,
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
    app.register_blueprint(
        create_media_blueprint(
            _get_db,
            json_error,
            db_path,
            app.config,
            invalid_preview_cache_paths=render_cache.invalid_preview_cache_paths,
            clear_preview_cache_invalid=render_cache.clear_preview_cache_invalid,
        )
    )
    # The prepare-full-resolution job calls the /original view directly so
    # its RAW/companion/edit fallbacks cannot drift from the lightbox's.
    serve_original_photo = app.view_functions["media.serve_original_photo"]

    app.register_blueprint(create_photo_labels_blueprint(_get_db, json_error))
    app.register_blueprint(create_photo_review_blueprint(_get_db, json_error))
    app.register_blueprint(create_life_list_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_audit_blueprint(
            _get_db,
            json_error,
            app.config,
            cleanup_cached_files_for_deleted_photos=(
                photo_deletion.cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=missing_originals.invalidate,
            get_runner=lambda: app._job_runner,
        )
    )
    app.register_blueprint(
        create_settings_blueprint(
            _get_db,
            json_error,
            app.config,
            advance_inat_token_generation=inat_token_generation.advance,
        )
    )
    app.register_blueprint(
        create_inat_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            token_generation=inat_token_generation,
        )
    )
    app.register_blueprint(
        create_storage_blueprint(_get_db, json_error, db_path, app.config)
    )
    app.register_blueprint(
        create_workspace_blueprint(
            _get_db,
            json_error,
            ALL_PAGES,
            get_runner=lambda: app._job_runner,
            invalidate_missing_originals=missing_originals.invalidate,
            new_images_walk_progress=app._new_images_walk_progress,
        )
    )
    app.register_blueprint(
        create_folders_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            build_scan_work=build_scan_work,
            cleanup_cached_files_for_deleted_photos=(
                photo_deletion.cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=missing_originals.invalidate,
        )
    )
    app.register_blueprint(create_capture_time_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_remote_setup_blueprint(_get_db, json_error, app.config)
    )
    app.register_blueprint(create_editing_blueprint(_get_db, json_error))
    app.register_blueprint(create_species_blueprint(_get_db))
    app.register_blueprint(
        create_system_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            get_log_broadcaster=lambda: app._log_broadcaster,
        )
    )
    app.register_blueprint(
        create_misses_blueprint(
            _get_db, json_error, resolve_visual=visual_scope.resolve,
        )
    )
    app.register_blueprint(
        create_browse_blueprint(_get_db, json_error, visual_scope=visual_scope)
    )
    app.register_blueprint(
        create_predictions_blueprint(
            _get_db, json_error, app.config, visual_scope=visual_scope,
        )
    )
    app.register_blueprint(create_encounters_blueprint(_get_db, json_error, db_path))
    # Registered before the /api/v1 alias loop below, which aliases two of
    # these endpoints by their ``collections.``-qualified names.
    app.register_blueprint(create_collections_blueprint(_get_db, json_error))
    app.register_blueprint(create_dashboard_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_sync_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
        )
    )
    app.register_blueprint(create_move_cleanup_blueprint(
        _get_db, lambda: app._job_runner, json_error,
        lambda paths: _trash_paths(paths), folder_moves.guard_error,
    ))
    app.register_blueprint(create_moves_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_duplicates_blueprint(
            _get_db,
            json_error,
            # Late-bound through the module globals, like the move-cleanup
            # blueprint's trash hook above, so a patched ``app._trash_paths``
            # or ``app._network_volume_roots`` still reaches these routes.
            trash_paths=lambda *args, **kwargs: _trash_paths(*args, **kwargs),
            network_volume_roots=lambda: _network_volume_roots(),
            path_on_network_volume=_path_on_network_volume,
            cleanup_cached_files_for_deleted_photos=(
                photo_deletion.cleanup_cached_files_for_deleted_photos
            ),
            invalidate_missing_originals=missing_originals.invalidate,
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
            invalidate_missing_originals=missing_originals.invalidate,
        )
    )
    app.register_blueprint(
        create_imports_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=missing_originals.invalidate,
            enqueue_process_job=pipeline_chain.enqueue_process_job,
            chain_after_move=pipeline_chain.chain_after_move,
            bulk_gps_location_payload=bulk_gps_locations.payload,
            guard_move_folder=folder_moves.guard_error,
            sync_job_lock=app._sync_job_lock,
        )
    )
    app.register_blueprint(create_keywords_blueprint(_get_db, json_error))
    app.register_blueprint(
        create_locations_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            normalize_photo_id_list=bulk_gps_locations.normalize_photo_id_list,
            bulk_gps_location_source_ids=bulk_gps_locations.source_ids,
        )
    )
    app.register_blueprint(
        create_batch_blueprint(
            _get_db,
            json_error,
            location_errors=location_errors,
            bulk_gps_location_payload=bulk_gps_locations.payload,
            run_batch_delete=photo_deletion.run_batch_delete,
            invalidate_missing_originals=missing_originals.invalidate,
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
            missing_originals_payload=missing_originals.payload,
            start_missing_originals_scan=missing_originals.start_scan,
            invalidate_missing_originals=missing_originals.invalidate,
            run_batch_delete=photo_deletion.run_batch_delete,
        )
    )
    app.register_blueprint(
        create_photo_edit_recipes_blueprint(
            _get_db,
            json_error,
            app.config,
            invalidate_photo_render_cache=render_cache.invalidate_photo_render_cache,
        )
    )
    app.register_blueprint(
        create_history_blueprint(
            _get_db,
            json_error,
            db_path,
            invalidate_photo_render_cache=render_cache.invalidate_photo_render_cache,
        )
    )
    app.register_blueprint(
        create_photo_location_keywords_blueprint(
            _get_db, json_error, location_errors=location_errors,
        )
    )
    app.register_blueprint(
        create_job_launchers_blueprint(
            _get_db,
            json_error,
            lambda: app._job_runner,
            db_path,
            app.config,
            invalidate_missing_originals=missing_originals.invalidate,
            run_batch_delete=photo_deletion.run_batch_delete,
            build_scan_work=build_scan_work,
            pending_local_workspace_transition=(
                folder_moves.pending_local_workspace_transition
            ),
            guard_move_folder=folder_moves.guard_error,
            start_move_folder_job=folder_moves.start_job,
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
            1.0, startup.retire_wildlife_genre,
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
