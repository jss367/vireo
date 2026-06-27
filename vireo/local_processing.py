"""Local staging helpers for import-then-archive pipeline runs."""

from __future__ import annotations

import math
import ntpath
import os
import shutil
from pathlib import Path

from ingest import discover_source_files

GIB = 1024 ** 3
DERIVED_OVERHEAD_RATIO = 0.25
MIN_DERIVED_OVERHEAD_BYTES = 5 * GIB
RESERVED_FREE_BYTES = 10 * GIB


def final_destination_name(final_destination: str) -> str:
    """Return the folder name that must land exactly at ``final_destination``."""
    nt_normalized = ntpath.normpath(final_destination)
    nt_drive, nt_tail = ntpath.splitdrive(nt_normalized)
    if nt_drive:
        final_name = ntpath.basename(nt_normalized)
        if not final_name or nt_tail in ("", "\\", "/"):
            raise ValueError("final_destination must not be a filesystem root")
        return final_name

    final_name = os.path.basename(os.path.normpath(final_destination))
    if not final_name:
        raise ValueError("final_destination must not be a filesystem root")
    return final_name


def staging_root(vireo_dir: str, job_id: str, final_destination: str) -> str:
    """Return the local root whose move target is ``final_destination``.

    ``move_folder`` always places the source folder inside the destination
    parent, preserving the source folder's name. Naming the staging root after
    the final destination's basename lets the verified archive step land at
    exactly the user-selected path.
    """
    final_name = final_destination_name(final_destination)
    return str(Path(vireo_dir) / "staging" / job_id / final_name)


def selected_source_files(
    sources: list[str],
    file_types: str = "both",
    recursive: bool = True,
    exclude_paths: set[str] | None = None,
) -> list[Path]:
    """Resolve the exact files a copy-mode pipeline import will consider."""
    excluded = exclude_paths or set()
    files: list[Path] = []
    for source in sources:
        for path in discover_source_files(source, file_types, recursive=recursive):
            if str(path) not in excluded:
                files.append(path)
    return files


def total_file_bytes(files: list[Path]) -> int:
    total = 0
    for path in files:
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def non_duplicate_bytes(files: list[Path], known_hashes: set[str]) -> int:
    """Sum bytes of ``files`` whose content hash isn't already known.

    Mirrors the duplicate gate ingest() applies with skip_duplicates=True so
    the local-storage preflight estimate matches what ingest will actually
    copy. ``known_hashes`` covers files already in the catalog; this also
    tracks hashes seen earlier in ``files`` so intra-run duplicates (the
    same card folder selected twice, or two source folders sharing a file)
    are counted once — ingest() likewise copies the first occurrence and
    skips later matches via its ``extra_known_hashes`` accumulator.
    """
    from scanner import compute_file_hash

    seen = set(known_hashes)
    total = 0
    for path in files:
        try:
            file_hash = compute_file_hash(str(path))
        except OSError:
            continue
        if file_hash in seen:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total += size
        seen.add(file_hash)
    return total


def estimate_required_bytes(source_bytes: int) -> int:
    """Estimate local working-set size for originals plus derived files."""
    derived = max(
        int(math.ceil(source_bytes * DERIVED_OVERHEAD_RATIO)),
        MIN_DERIVED_OVERHEAD_BYTES if source_bytes else 0,
    )
    return source_bytes + derived


def storage_plan(
    staging_dir: str,
    source_bytes: int,
    *,
    archive_parent: str | None = None,
    reserved_free_bytes: int | None = None,
) -> dict:
    """Return local-storage availability and whether batching is required.

    ``reserved_free_bytes`` defaults to the module-level constant, looked up
    at call time so monkeypatching it in tests actually takes effect.

    When ``archive_parent`` is supplied the plan also accounts for the
    destination volume: archive runs copy-verify-delete via ``move_folder``,
    so the staged originals must briefly coexist at the destination before
    staging is removed. When both paths live on the same device, the
    staging volume must hold the originals once for staging, the derived
    files, AND a second copy for the destination — without doubling
    ``source_bytes`` the run could pass the preflight, process for hours,
    and then fail in the final move with ENOSPC. When they live on
    different devices, the destination volume gets an independent free
    space check.
    """
    if reserved_free_bytes is None:
        reserved_free_bytes = RESERVED_FREE_BYTES

    same_device = False
    if archive_parent:
        try:
            same_device = (
                os.stat(staging_dir).st_dev == os.stat(archive_parent).st_dev
            )
        except OSError:
            same_device = False

    usage = shutil.disk_usage(staging_dir)
    required = estimate_required_bytes(source_bytes)
    if same_device:
        # Both staging copy and destination copy land on this volume; the
        # destination copy is taken from staging before the staging files
        # are deleted, so the peak working set is staging + derived +
        # destination = required + source_bytes.
        required += source_bytes
    usable = max(0, usage.free - reserved_free_bytes)
    staging_enough = required <= usable

    archive_required = 0
    archive_free = None
    archive_usable = None
    archive_enough = True
    if archive_parent and not same_device:
        archive_required = source_bytes
        try:
            archive_usage = shutil.disk_usage(archive_parent)
        except OSError:
            # Can't probe the destination volume — let the eventual
            # move_folder surface ENOSPC rather than guess. Leave
            # archive_enough True so the preflight doesn't false-positive.
            pass
        else:
            archive_free = archive_usage.free
            archive_usable = max(0, archive_usage.free - reserved_free_bytes)
            archive_enough = archive_required <= archive_usable

    enough = staging_enough and archive_enough
    if enough:
        batch_count = 1
        batch_bytes = source_bytes
    else:
        # Estimate how many source-byte batches fit after reserving room for
        # derived files. The caller can use this for user-facing messaging even
        # before full batch execution is available.
        per_batch_source = max(
            1,
            int(usable / (1 + DERIVED_OVERHEAD_RATIO + (1 if same_device else 0))),
        )
        batch_count = math.ceil(source_bytes / per_batch_source) if source_bytes else 1
        batch_bytes = per_batch_source
    return {
        "source_bytes": source_bytes,
        "required_bytes": required,
        "free_bytes": usage.free,
        "reserved_free_bytes": reserved_free_bytes,
        "usable_bytes": usable,
        "staging_enough": staging_enough,
        "archive_required_bytes": archive_required,
        "archive_free_bytes": archive_free,
        "archive_usable_bytes": archive_usable,
        "archive_enough": archive_enough,
        "same_device": same_device,
        "enough": enough,
        "batching_required": not enough,
        "batch_count": int(batch_count),
        "estimated_batch_source_bytes": int(batch_bytes),
    }


def format_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
