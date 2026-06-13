"""ExifTool-based metadata extraction.

Wraps the exiftool binary to extract comprehensive metadata from photo files.
Returns grouped tag dictionaries keyed by ExifTool group (EXIF, GPS, XMP, etc.).
"""

import json
import logging
import subprocess
import sys

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

    cmd = ["exiftool", "-G", "-json", "-n"]
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


def extract_metadata(file_paths, restricted_tags=None):
    """Extract metadata from files using ExifTool.

    Args:
        file_paths: list of file path strings
        restricted_tags: optional list of tag names to extract (e.g.
            ['-DateTimeOriginal', '-GPSLatitude', '-ImageWidth']).
            If None, extracts all tags.

    Returns:
        dict mapping file_path -> grouped metadata dict.
        Files that failed extraction are omitted from the result.
    """
    if not file_paths:
        return {}

    results = {}
    # Process in batches
    for i in range(0, len(file_paths), _BATCH_SIZE):
        batch = file_paths[i:i + _BATCH_SIZE]
        raw = _run_exiftool_with_retries(batch, extra_args=restricted_tags)
        for entry in raw:
            source = entry.get("SourceFile")
            if source:
                results[source] = _group_tags(entry)

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
