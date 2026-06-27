"""Local staging helpers for import-then-archive pipeline runs."""

from __future__ import annotations

import math
import os
import shutil
from pathlib import Path

from ingest import discover_source_files

GIB = 1024 ** 3
DERIVED_OVERHEAD_RATIO = 0.25
MIN_DERIVED_OVERHEAD_BYTES = 5 * GIB
RESERVED_FREE_BYTES = 10 * GIB


def staging_root(vireo_dir: str, job_id: str, final_destination: str) -> str:
    """Return the local root whose move target is ``final_destination``.

    ``move_folder`` always places the source folder inside the destination
    parent, preserving the source folder's name. Naming the staging root after
    the final destination's basename lets the verified archive step land at
    exactly the user-selected path.
    """
    final_name = os.path.basename(os.path.normpath(final_destination)) or "archive"
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
    """Sum bytes of ``files`` whose content hash isn't in ``known_hashes``.

    Mirrors the duplicate gate ingest() applies with skip_duplicates=True so
    the local-storage preflight estimate matches what ingest will actually
    copy. Without this filter a card that's mostly already-imported photos
    would set ``batching_required`` and abort even though the staging copy
    would fit (or be zero bytes).
    """
    from scanner import compute_file_hash

    total = 0
    for path in files:
        try:
            file_hash = compute_file_hash(str(path))
        except OSError:
            continue
        if file_hash in known_hashes:
            continue
        try:
            total += path.stat().st_size
        except OSError:
            continue
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
    reserved_free_bytes: int | None = None,
) -> dict:
    """Return local-storage availability and whether batching is required.

    ``reserved_free_bytes`` defaults to the module-level constant, looked up
    at call time so monkeypatching it in tests actually takes effect.
    """
    if reserved_free_bytes is None:
        reserved_free_bytes = RESERVED_FREE_BYTES
    usage = shutil.disk_usage(staging_dir)
    required = estimate_required_bytes(source_bytes)
    usable = max(0, usage.free - reserved_free_bytes)
    if required <= usable:
        batch_count = 1
        batch_bytes = source_bytes
    else:
        # Estimate how many source-byte batches fit after reserving room for
        # derived files. The caller can use this for user-facing messaging even
        # before full batch execution is available.
        per_batch_source = max(
            1,
            int(usable / (1 + DERIVED_OVERHEAD_RATIO)),
        )
        batch_count = math.ceil(source_bytes / per_batch_source) if source_bytes else 1
        batch_bytes = per_batch_source
    return {
        "source_bytes": source_bytes,
        "required_bytes": required,
        "free_bytes": usage.free,
        "reserved_free_bytes": reserved_free_bytes,
        "usable_bytes": usable,
        "enough": required <= usable,
        "batching_required": required > usable,
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
