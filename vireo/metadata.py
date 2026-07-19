"""ExifTool-based metadata extraction.

Wraps the exiftool binary to extract comprehensive metadata from photo files.
Returns grouped tag dictionaries keyed by ExifTool group (EXIF, GPS, XMP, etc.).
"""

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

try:
    from .proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

log = logging.getLogger(__name__)

# Batch size for ExifTool invocations
_BATCH_SIZE = 100
_EXIFTOOL_TIMEOUT = 120
_MAX_TIMEOUT_SPLIT_ATTEMPTS = 16
_TIMEOUT = object()


def find_exiftool() -> str | None:
    """Resolve ExifTool, preferring Vireo's packaged Windows copy.

    PyInstaller extracts bundled data below ``sys._MEIPASS``.  Keeping the
    support directory next to the executable is required by the official
    Windows distribution.  Development installs continue to use PATH.
    """
    bundle_root = getattr(sys, "_MEIPASS", None)
    if bundle_root:
        bundled = Path(bundle_root) / "vendor" / "exiftool" / "exiftool.exe"
        if bundled.is_file():
            return str(bundled)
    try:
        return shutil.which("exiftool")
    except (AttributeError, OSError):
        # Defensive for restricted Windows runtimes where executable lookup
        # itself is unavailable. Callers that execute the fallback still
        # receive the normal FileNotFoundError handling.
        return None


def _exiftool_install_hint() -> str:
    """Platform-appropriate guidance for installing exiftool.

    The macOS-only ``brew install exiftool`` hint is wrong on Windows
    (no Homebrew) and Linux, so tailor the message per platform.
    """
    if sys.platform == "win32":
        return "download it from https://exiftool.org"
    if sys.platform == "darwin":
        return "install it with: brew install exiftool"
    return "install it with your package manager (e.g. apt install libimage-exiftool-perl)"


def exiftool_available():
    """Return True if the exiftool binary resolves on PATH AND runs.

    A PATH-only check misses broken installs (dangling wrapper, bad Perl)
    where ``shutil.which`` succeeds but ``exiftool -ver`` fails — in that
    case every scan loses metadata silently. Probes via ``-ver``; called
    once per scan, not per photo.
    """
    return exiftool_status()["available"]


def exiftool_status():
    """Report exiftool presence, runnability, path, version, and a hint.

    Mirrors the shape of ``/api/darktable/status`` so the UI can render
    external-dependency checks uniformly. ``available`` is True only when
    the binary both resolves on PATH and a ``-ver`` probe succeeds — a
    PATH-only check would render broken installs as a healthy green state
    while every scan loses capture date, GPS, and camera info.
    """
    path = find_exiftool()
    if not path:
        return {
            "available": False,
            "path": "",
            "version": None,
            "error": None,
            "hint": _exiftool_install_hint(),
        }

    ran_ok = False
    version = None
    error = None
    try:
        result = subprocess.run(
            [path, "-ver"],
            capture_output=True,
            text=True,
            timeout=10,
            **no_window_kwargs(),
        )
        if result.returncode == 0:
            ran_ok = True
            version = result.stdout.strip() or None
        else:
            error = (
                result.stderr.strip()
                or f"exiftool -ver exited with status {result.returncode}"
            )
    except Exception as e:
        error = str(e) or e.__class__.__name__

    if ran_ok:
        return {
            "available": True,
            "path": path,
            "version": version,
            "error": None,
            "hint": _exiftool_install_hint(),
        }

    return {
        "available": False,
        "path": path,
        "version": None,
        "error": error,
        "hint": (
            f"found at {path} but couldn't run — reinstall it "
            f"({_exiftool_install_hint()})"
        ),
    }


def scan_metadata_warning():
    """Return a user-facing warning when exiftool is unavailable, else ``None``.

    Appended to a scan step's summary so a scan run without a runnable
    exiftool reads as "completed, but degraded" instead of a clean success
    — photos indexed in that run get no capture date, GPS, or camera info.
    Reused by every scan completion path (standalone scan, import, and
    pipeline) so silent degradation is closed off uniformly.
    """
    if exiftool_available():
        return None
    return "⚠ ExifTool not found — no capture dates, GPS, or camera info"


def _run_exiftool(file_paths, extra_args=None):
    """Run exiftool on a list of files, return parsed JSON output.

    Args:
        file_paths: list of file path strings
        extra_args: optional list of additional exiftool arguments

    Returns:
        list of dicts (one per file) from exiftool JSON output
    """
    if not file_paths:
        return []

    exiftool = find_exiftool() or "exiftool"
    cmd = [exiftool, "-G", "-json", "-n"]
    if extra_args:
        cmd.extend(extra_args)
    cmd.append("--")
    cmd.extend(file_paths)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_EXIFTOOL_TIMEOUT,
            **no_window_kwargs(),
        )
        if result.returncode not in (0, 1):
            # returncode 1 = warnings (e.g. minor errors), still has output
            log.warning("exiftool returned %d: %s", result.returncode, result.stderr[:200])
            return []
        if result.stdout.strip():
            return json.loads(result.stdout)
        return []
    except FileNotFoundError:
        log.error("exiftool not found — %s", _exiftool_install_hint())
    except subprocess.TimeoutExpired:
        log.error("exiftool timed out processing %d files", len(file_paths))
        return _TIMEOUT
    except json.JSONDecodeError as e:
        log.error("Failed to parse exiftool JSON output: %s", e)

    return []


def _run_exiftool_with_retries(file_paths, extra_args=None, split_budget=None):
    """Run ExifTool, splitting timed-out batches to salvage metadata."""
    if split_budget is None:
        split_budget = [_MAX_TIMEOUT_SPLIT_ATTEMPTS]

    raw = _run_exiftool(file_paths, extra_args=extra_args)
    if raw is not _TIMEOUT:
        return raw

    if len(file_paths) <= 1:
        return []

    if split_budget[0] <= 0:
        log.warning(
            "Stopping exiftool timeout retries for %d files after exhausting split budget",
            len(file_paths),
        )
        return []

    split_budget[0] -= 1
    mid = len(file_paths) // 2
    log.warning(
        "Retrying timed-out exiftool batch of %d files as %d + %d (%d splits left)",
        len(file_paths),
        mid,
        len(file_paths) - mid,
        split_budget[0],
    )
    return (
        _run_exiftool_with_retries(
            file_paths[:mid], extra_args=extra_args, split_budget=split_budget
        )
        + _run_exiftool_with_retries(
            file_paths[mid:], extra_args=extra_args, split_budget=split_budget
        )
    )


def _group_tags(flat_dict):
    """Convert exiftool flat 'Group:Tag' keys into nested {Group: {Tag: value}}.

    ExifTool with -G outputs keys like 'EXIF:Make', 'GPS:GPSLatitude'.
    SourceFile and other ungrouped keys go into a '_meta' group.
    """
    grouped = {}
    for key, value in flat_dict.items():
        if key == "SourceFile":
            continue
        if ":" in key:
            group, tag = key.split(":", 1)
            grouped.setdefault(group, {})[tag] = value
        else:
            grouped.setdefault("_meta", {})[key] = value
    return grouped


def extract_metadata(file_paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
    """Extract metadata from files using ExifTool.

    Args:
        file_paths: list of file path strings
        restricted_tags: optional list of tag names to extract (e.g.
            ['-DateTimeOriginal', '-GPSLatitude', '-ImageWidth']).
            If None, extracts all tags.
        progress_callback: optional callable(current, total) invoked after
            each ExifTool batch completes.
        checkpoint: optional zero-argument callable invoked before and after
            every ExifTool batch. It may block (pause) or raise (cancel).

    Returns:
        dict mapping file_path -> grouped metadata dict.
        Files that failed extraction are omitted from the result.
    """
    if not file_paths:
        return {}

    results = {}
    # Process in batches
    for i in range(0, len(file_paths), _BATCH_SIZE):
        if checkpoint:
            checkpoint()
        batch = file_paths[i:i + _BATCH_SIZE]
        raw = _run_exiftool_with_retries(batch, extra_args=restricted_tags)
        for entry in raw:
            source = entry.get("SourceFile")
            if source:
                results[source] = _group_tags(entry)
        if progress_callback:
            progress_callback(min(i + len(batch), len(file_paths)), len(file_paths))
        if checkpoint:
            checkpoint()

    return results


def extract_summary_fields(grouped_meta):
    """Pull quick-summary fields from grouped metadata.

    Args:
        grouped_meta: dict of {Group: {Tag: value}} as returned by extract_metadata

    Returns:
        dict with normalized summary keys, None for missing values.
    """
    exif = grouped_meta.get("EXIF", {})
    composite = grouped_meta.get("Composite", {})

    return {
        "camera_make": exif.get("Make"),
        "camera_model": exif.get("Model"),
        "lens": composite.get("LensID") or exif.get("LensModel") or composite.get("Lens"),
        "focal_length": exif.get("FocalLength"),
        "f_number": exif.get("FNumber"),
        "exposure_time": exif.get("ExposureTime"),
        "iso": exif.get("ISO"),
        "datetime_original": exif.get("DateTimeOriginal"),
    }
