"""Local staging helpers for import-then-archive pipeline runs."""

from __future__ import annotations

import filecmp
import math
import ntpath
import os
import shutil
from pathlib import Path

from ingest import (
    _source_file_timestamps,
    build_destination_path,
    discover_source_files,
)

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


def _as_duplicate_checker(duplicates):
    """Normalize a duplicate-oracle argument to a DuplicateChecker.

    Callers with full catalog context pass a DuplicateChecker (built by
    pipeline_job over a CatalogIndex, so predictions here use the exact
    rules — and mode — the real ingest will). Legacy callers and tests
    that only have a bare hash set still work: it becomes a hash-only
    checker in verify mode, which is the historical exact behavior.
    DuplicateChecker inherits ingest()'s zero-byte rule — empty files
    carry no duplicate identity, so an earlier zero-byte source can't
    cause a later zero-byte source's same-path conflict to be falsely
    skipped, and the duplicate-filter survivor list can't collapse two
    empty sources ingest would both copy.
    """
    from import_dedup import CatalogIndex, DuplicateChecker

    if duplicates is None or isinstance(duplicates, DuplicateChecker):
        return duplicates
    return DuplicateChecker(
        CatalogIndex.from_hashes(duplicates), verify_by_hash=True,
    )


def _suffix_against(folder_taken: set[str], name: str) -> str:
    """Pick the next free slot for ``name`` given the per-folder set
    ``folder_taken``. Mirrors ingest()'s name-collision rule: the first
    source claims the unsuffixed name, then ``name_1.ext``, ``name_2.ext``,
    and so on for subsequent collisions in the same archive folder.
    """
    if name not in folder_taken:
        return name
    stem, suffix_ext = os.path.splitext(name)
    counter = 1
    while f"{stem}_{counter}{suffix_ext}" in folder_taken:
        counter += 1
    return f"{stem}_{counter}{suffix_ext}"


def conflicting_archive_paths(
    path: str,
    files: list[Path],
    folder_template: str = "%Y/%Y-%m-%d",
    *,
    known_hashes: set[str] | None = None,
) -> list[str]:
    """Return existing archive paths that would block merge-mode archive."""
    report = archive_conflict_report(
        path,
        files,
        folder_template,
        known_hashes=known_hashes,
    )
    return sorted(
        report["empty"] + report["partial"] + report["conflicts"]
    )


def archive_conflict_report(
    path: str,
    files: list[Path],
    folder_template: str = "%Y/%Y-%m-%d",
    *,
    known_hashes: set[str] | None = None,
    duplicate_checker=None,
    indexed_paths: set[str] | None = None,
) -> dict[str, list[str]]:
    """Return existing archive paths that would block merge-mode archive.

    ``move_folder(..., merge=True)`` refuses same-relative-path files whose
    contents differ. Detect those conflicts before the expensive staging and
    processing work begins.

    The check mirrors ``ingest()``'s staging behavior so it only flags real
    archive conflicts:

    * When ``duplicate_checker`` (an import_dedup.DuplicateChecker — pass a
      FRESH instance, its seen-state is consumed here) or ``known_hashes``
      (legacy bare hash set, checked in exact/verify mode) is provided,
      sources the checker recognizes as catalog duplicates are treated as
      ingest skips — they never reach staging, so they cannot conflict at
      the final archive step. Duplicate identity is checked before a
      source claims an archive filename so skipped duplicates cannot shift
      later survivors onto suffixed paths that ingest would not use.
    * When two surviving sources land in the same archive subfolder under
      the same filename, ingest stages the first at the unsuffixed name
      and the second under ``name_1.ext`` (then ``name_2.ext`` ...). The
      preflight tracks per-folder occupied names so the existing archive's
      same-name file is only compared to the first survivor's content,
      not also (incorrectly) to the second survivor's.
    * Earlier survivors are recorded in the checker as they claim names.
      Mirrors ingest()'s shared-checker accumulator so the same
      card/folder selected twice still recognises the second occurrence
      as an intra-batch duplicate that ingest would skip.

    If ``indexed_paths`` is provided, same-path differences are split into:

    * ``empty``: unindexed zero-byte archive files with a non-empty source.
    * ``partial``: unindexed archive files smaller than the source.
    * ``conflicts``: everything else that still blocks the merge.

    Empty/partial files are still blocking. They are separated so callers can
    tell the user this looks like debris from a failed previous archive copy,
    not a legitimate same-name alternate photo.
    """
    try:
        if not os.path.isdir(path):
            return {"empty": [], "partial": [], "conflicts": []}
    except OSError:
        return {"empty": [], "partial": [], "conflicts": []}

    timestamps = _source_file_timestamps(files)
    archive_root = Path(path)
    empty: set[str] = set()
    partial: set[str] = set()
    conflicts: set[str] = set()
    indexed_keys = (
        {
            os.path.normcase(os.path.abspath(indexed_path))
            for indexed_path in indexed_paths
        }
        if indexed_paths is not None else None
    )
    checker = duplicate_checker or _as_duplicate_checker(known_hashes)
    if checker is not None:
        checker.prepare(files)
    # Per-archive-subfolder name slots already claimed by earlier survivors
    # in this iteration. Mirrors the per-batch staging tree ingest builds.
    occupied: dict[str, set[str]] = {}

    def _skip_or_record_survivor(source_file: Path) -> bool:
        """Return True when ingest would skip ``source_file`` as a duplicate."""
        if checker is None:
            return False
        try:
            return checker.check_and_record(source_file)
        except OSError:
            # ingest() treats identity-check failures as per-file failures
            # before copy, so they do not claim destination names.
            return True

    for source_file in files:
        try:
            rel_folder = build_destination_path(
                timestamps.get(source_file),
                folder_template,
            )
            dest_folder = archive_root / rel_folder
            folder_key = os.path.normcase(os.path.abspath(dest_folder))
            folder_taken = occupied.setdefault(folder_key, set())

            chosen_name = _suffix_against(folder_taken, source_file.name)

            dest_file = dest_folder / chosen_name
            if not os.path.lexists(dest_file):
                if _skip_or_record_survivor(source_file):
                    continue
                folder_taken.add(chosen_name)
                continue

            source_size = source_file.stat().st_size
            dest_size: int | None = None
            if not dest_file.is_symlink() and dest_file.is_file():
                dest_size = dest_file.stat().st_size
                if (
                    dest_size == source_size
                    and filecmp.cmp(source_file, dest_file, shallow=False)
                ):
                    if _skip_or_record_survivor(source_file):
                        continue
                    folder_taken.add(chosen_name)
                    continue

            # Same-path archive file with different content. If ingest
            # would skip this source as a known duplicate, it never
            # reaches staging and so cannot conflict — confirm before
            # rejecting the run.
            if _skip_or_record_survivor(source_file):
                # ingest's skip_duplicates branch drops this source
                # before staging; do not claim a slot either.
                continue

            dest_path = str(dest_file)
            dest_key = os.path.normcase(os.path.abspath(dest_path))
            is_unindexed = (
                indexed_keys is not None
                and dest_key not in indexed_keys
            )
            if is_unindexed and source_size > 0 and dest_size is not None:
                if dest_size == 0:
                    empty.add(dest_path)
                elif dest_size < source_size:
                    partial.add(dest_path)
                else:
                    conflicts.add(dest_path)
            else:
                conflicts.add(dest_path)
            folder_taken.add(chosen_name)
        except OSError:
            continue
    return {
        "empty": sorted(empty),
        "partial": sorted(partial),
        "conflicts": sorted(conflicts),
    }


def existing_archive_bytes(
    path: str,
    files: list[Path],
    folder_template: str = "%Y/%Y-%m-%d",
) -> int:
    """Sum source bytes already present under ``path`` for resume calculations.

    When a previous archive attempt left a partial untracked
    ``final_destination``, ``move_folder(..., merge=True)`` rsyncs only the
    files that aren't already there. Credit only destination files that match
    the selected sources at the same relative import path. Unrelated files in
    an existing NAS folder do not reduce the bytes the archive will write.

    When two selected files land in the same archive folder under the same
    basename, ingest() stages the first as ``name.ext`` and the second as
    ``name_1.ext``. A previous partial archive that already contains the
    suffixed file from a prior interrupted run must still earn resume credit
    for the second source — without mirroring ingest's suffix logic the
    second source always points back at the first's already-credited
    unsuffixed slot and the run can be batching-rejected even though the
    delta would fit.
    """
    try:
        if not os.path.isdir(path):
            return 0
    except OSError:
        return 0

    timestamps = _source_file_timestamps(files)
    total = 0
    credited: set[str] = set()
    archive_root = Path(path)
    # Per-archive-subfolder slots claimed by earlier sources in this
    # iteration. Mirrors the same per-folder accounting in
    # ``conflicting_archive_paths`` so both preflight checks agree on
    # which archive path a same-basename second source maps to.
    occupied: dict[str, set[str]] = {}
    for source_file in files:
        try:
            rel_folder = build_destination_path(
                timestamps.get(source_file),
                folder_template,
            )
            dest_folder = archive_root / rel_folder
            folder_key = os.path.normcase(os.path.abspath(dest_folder))
            folder_taken = occupied.setdefault(folder_key, set())
            chosen_name = _suffix_against(folder_taken, source_file.name)
            folder_taken.add(chosen_name)
            dest_file = dest_folder / chosen_name
            key = os.path.normcase(os.path.abspath(dest_file))
            if key in credited or dest_file.is_symlink() or not dest_file.is_file():
                continue
            source_size = source_file.stat().st_size
            if dest_file.stat().st_size != source_size:
                continue
            if not filecmp.cmp(source_file, dest_file, shallow=False):
                continue
            total += source_size
            credited.add(key)
        except OSError:
            continue
    return total


def non_duplicate_files(files: list[Path], duplicates) -> list[Path]:
    """Return ``files`` minus any the duplicate gate would skip.

    Mirrors the duplicate gate ingest() applies with skip_duplicates=True so
    the local-storage preflight estimate matches what ingest will actually
    copy. ``duplicates`` is an import_dedup.DuplicateChecker (pass a FRESH
    instance — its seen-state is consumed here) or a legacy bare hash set,
    normalized via _as_duplicate_checker. Files whose identity was seen
    earlier in ``files`` are dropped too, so intra-run duplicates (the same
    card folder selected twice, or two source folders sharing a file) yield
    only the first occurrence — ingest() likewise copies the first and
    skips later matches via its shared-checker accumulator.

    Returning the file list (not just a byte sum) lets callers feed the
    survivor set back into ``existing_archive_bytes`` so the destination
    credit on a duplicate-filtered retry reflects only files ingest will
    actually copy. Crediting the destination for a large duplicate that
    ingest will skip would zero out the archive delta and let the run pass
    the destination-space preflight even when the fresh files still need
    room at the archive.
    """
    checker = _as_duplicate_checker(duplicates)
    if checker is None:
        return list(files)
    checker.prepare(files)
    survivors: list[Path] = []
    for path in files:
        try:
            if checker.check_and_record(path):
                continue
        except OSError:
            continue
        survivors.append(path)
    return survivors


def non_duplicate_bytes(files: list[Path], duplicates) -> int:
    """Sum bytes of ``files`` the duplicate gate would let through."""
    return total_file_bytes(non_duplicate_files(files, duplicates))


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
    archive_existing_bytes: int = 0,
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

    When ``archive_existing_bytes`` is non-zero it tells the planner that a
    previous archive attempt already left that many bytes at the
    destination. ``move_folder(..., merge=True)`` skips files that are
    already present, so a retry only needs space for the remaining bytes —
    capped at ``source_bytes`` so unrelated content at the destination
    can't extend an unbounded credit. Without this credit a retry whose
    delta would fit gets rejected as batching-required and the user has
    to delete the partial archive by hand before resuming.
    """
    if reserved_free_bytes is None:
        reserved_free_bytes = RESERVED_FREE_BYTES

    # Cap at source_bytes so an oversized or unrelated destination tree
    # doesn't inflate the credit beyond what merge-mode can actually skip.
    existing_credit = max(0, min(archive_existing_bytes, source_bytes))
    archive_delta = max(0, source_bytes - existing_credit)

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
        # are deleted. In merge-mode resumes, only the delta is rewritten
        # at the destination, so the peak working set on this volume is
        # staging + derived + delta.
        required += archive_delta
    usable = max(0, usage.free - reserved_free_bytes)
    staging_enough = required <= usable

    archive_required = 0
    archive_free = None
    archive_usable = None
    archive_enough = True
    if archive_parent and not same_device:
        archive_required = archive_delta
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
        "archive_existing_bytes": existing_credit,
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
