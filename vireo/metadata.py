"""ExifTool-based metadata extraction.

Wraps the exiftool binary to extract comprehensive metadata from photo files.
Returns grouped tag dictionaries keyed by ExifTool group (EXIF, GPS, XMP, etc.).
"""

import json
import logging
import os
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
_MACOS_HOMEBREW_BIN_DIRS = ("/opt/homebrew/bin", "/usr/local/bin")

# Cached ``-ver`` probe results for bundled ExifTool paths. Repeating the
# probe on every scan batch (~100/scan) would add subprocess overhead, so
# results are memoized per resolved path. :func:`clear_exiftool_cache`
# resets it after the in-app Repair action installs a PATH copy.
_BUNDLED_PROBE_CACHE: dict[str, bool] = {}


def find_exiftool() -> str | None:
    """Resolve ExifTool, preferring Vireo's packaged desktop copy.

    PyInstaller extracts bundled data below ``sys._MEIPASS``.  Keeping the
    support directory next to the executable is required by the official
    Windows distribution.  Development installs continue to use PATH.

    If the bundled binary is present but fails a ``-ver`` probe (for
    example its ``lib`` directory was corrupted), PATH and Homebrew's
    standard macOS locations are consulted as fallbacks. GUI-launched
    apps often inherit a PATH without ``/opt/homebrew/bin``, even though
    the Repair action can invoke Homebrew there directly. The bundled
    probe result is cached per-path so hot scan batches don't reprobe
    every invocation.
    """
    bundle_root = getattr(sys, "_MEIPASS", None)
    if bundle_root:
        names = ("exiftool.exe",) if sys.platform == "win32" else ("exiftool",)
        for name in names:
            bundled = Path(bundle_root) / "vendor" / "exiftool" / name
            if bundled.is_file():
                if _bundled_exiftool_works(str(bundled)):
                    return str(bundled)
                # Bundled copy is present but broken — fall through to PATH.
                break
    try:
        found = shutil.which("exiftool")
    except (AttributeError, OSError):
        # Defensive for restricted Windows runtimes where executable lookup
        # itself is unavailable. Callers that execute the fallback still
        # receive the normal FileNotFoundError handling.
        found = None
    if found:
        return found
    if sys.platform == "darwin":
        return _find_standard_homebrew_tool("exiftool")
    return None


def _bundled_exiftool_works(path: str) -> bool:
    """Return True if a ``-ver`` probe of a bundled ExifTool succeeds."""
    cached = _BUNDLED_PROBE_CACHE.get(path)
    if cached is not None:
        return cached
    ok = _probe_exiftool_ver(path)[0]
    _BUNDLED_PROBE_CACHE[path] = ok
    return ok


def clear_exiftool_cache() -> None:
    """Reset the bundled-ExifTool probe cache.

    Called after the in-app Repair action so a freshly installed PATH
    ExifTool becomes visible without an app restart even when the
    previous bundled probe was cached.
    """
    _BUNDLED_PROBE_CACHE.clear()


def _probe_exiftool_ver(path: str) -> tuple[bool, str | None, str | None]:
    """Run ``exiftool -ver`` at ``path`` and return (ok, version, error)."""
    try:
        result = subprocess.run(
            [*_exiftool_command(path), "-ver"],
            capture_output=True,
            text=True,
            timeout=10,
            **no_window_kwargs(),
        )
        if result.returncode == 0:
            return True, (result.stdout.strip() or None), None
        return (
            False,
            None,
            (
                result.stderr.strip()
                or f"exiftool -ver exited with status {result.returncode}"
            ),
        )
    except Exception as e:
        return False, None, str(e) or e.__class__.__name__


def _exiftool_command(path: str | None = None) -> list[str]:
    """Return an executable argv prefix for a resolved ExifTool path.

    PyInstaller may extract a data file without its executable bit.  The
    bundled macOS distribution is a Perl script, so invoking it explicitly
    through the system Perl interpreter keeps the signed app reliable across
    PyInstaller versions and temporary extraction filesystems.
    """
    path = path or find_exiftool() or "exiftool"
    bundle_root = getattr(sys, "_MEIPASS", None)
    is_bundled_unix = (
        sys.platform != "win32"
        and bundle_root
        and Path(path).parent == Path(bundle_root) / "vendor" / "exiftool"
    )
    if is_bundled_unix or (sys.platform != "win32" and not os.access(path, os.X_OK)):
        return ["/usr/bin/perl", path]
    return [path]


def find_homebrew() -> str | None:
    """Resolve Homebrew from PATH or its standard GUI-app locations."""
    found = shutil.which("brew")
    if found:
        return found
    return _find_standard_homebrew_tool("brew")


def _find_standard_homebrew_tool(name: str) -> str | None:
    """Find an executable in Homebrew's Apple Silicon or Intel prefix."""
    for bin_dir in _MACOS_HOMEBREW_BIN_DIRS:
        candidate = os.path.join(bin_dir, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
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

    ran_ok, version, error = _probe_exiftool_ver(path)

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

    cmd = [*_exiftool_command(), "-G", "-json", "-n"]
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


# Promoted EXIF summary columns in the ``photos`` table — the canonical
# list the scanner uses to clear absent fields on rescan (a metadata write
# that omits a column should reset it to NULL, not leave the stale value
# behind). Kept next to ``exif_summary_columns`` so the two stay in lockstep.
EXIF_SUMMARY_COLUMNS = (
    "camera_make",
    "camera_model",
    "lens",
    "aperture",
    "shutter_speed",
    "iso",
)


def exif_summary_columns(grouped_meta):
    """Map grouped metadata to the photos-table EXIF summary columns.

    Returns {column: value} with only present, type-valid entries — safe to
    splice into an UPDATE. String fields are stripped; numeric fields are
    coerced (ExifTool runs with -n so values are usually numeric already,
    but sidecar-sourced or vendor-quirk values can be strings or lists).
    """
    summary = extract_summary_fields(grouped_meta)
    columns = {}

    def _text(value):
        if isinstance(value, int | float) and not isinstance(value, bool):
            value = str(value)
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

    def _number(value):
        if isinstance(value, list) and value:
            value = value[0]
        if isinstance(value, bool):
            return None
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                return None
        return None

    for column, key in (
        ("camera_make", "camera_make"),
        ("camera_model", "camera_model"),
        ("lens", "lens"),
    ):
        value = _text(summary.get(key))
        if value is not None:
            columns[column] = value
    for column, key in (
        ("aperture", "f_number"),
        ("shutter_speed", "exposure_time"),
    ):
        value = _number(summary.get(key))
        if value is not None:
            columns[column] = value
    iso = _number(summary.get("iso"))
    if iso is not None:
        columns["iso"] = int(iso)
    return columns
