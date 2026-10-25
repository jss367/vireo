"""Catalog twins of a source file, and linking the folders of accepted
duplicate twins into the workspace.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from import_dedup import stored_metadata_key
from import_job.cataloging import _invalidate_new_images
from import_job.state import _counts
from services import workspace_folders

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from db import Database
    from import_job.import_types import DupToken, Emit
    from import_job.state import _ImportBatchState, _ImportRunState

log = logging.getLogger(__name__)


def _link_twins_and_emit(
    state: _ImportRunState, batch_st: _ImportBatchState, db: Database, workspace_id: int,
    emit: Emit, rel: str, queued: int,
) -> None:
    """Link verified duplicate-twin folders into the workspace and emit
    the end-of-batch progress line. Shared verbatim by both import
    paths; the caller keeps the ``if state.cancelled: break``.
    """
    # A verified duplicate skip's twin folder may live elsewhere under
    # the archive (older date-layout, ``unsorted``, etc.). The twins
    # already have catalog rows and _linkable_twin_dirs has checked
    # that their folders exist under this destination — link those
    # rows directly. A broad incremental scan would still
    # enumerate/stat every file in the matched folders before the
    # import could finish (over SMB that can turn a zero-copy import
    # into an hours-long metadata walk); uncataloged-stray repair
    # belongs to the explicit folder-rescan workflow instead.
    new_dup_dirs = batch_st.dup_dirs - state.linked_dup_dirs
    if new_dup_dirs:
        linked, failures = _link_duplicate_twin_dirs(
            db, workspace_id, new_dup_dirs,
        )
        state.linked_dup_dirs.update(linked)
        if failures:
            state.dup_link_failed = True
            for d, detail in failures.items():
                state.unsafe_files.append({
                    "path": d,
                    "reason": (
                        "duplicate-folder workspace link failed: "
                        f"{detail}"
                    ),
                })
    emit(
        f"{rel}: {_counts(state, rel)['copied']} copied · "
        f"{_counts(state, rel)['skipped_duplicate']} already present",
        state.emitted, queued,
    )


def _key_twin_rows(db: Database, key: tuple[str, int, str]) -> list[sqlite3.Row]:
    """Catalog rows whose stored identity equals a source metadata key.

    ``CatalogIndex`` retains only identity sets, so the safe-to-format
    second pass resolves a key match's cataloged twin(s) here. Prefilter
    by file_size (cheap, indexedable) and compare the full
    ``stored_metadata_key`` in Python — SQL LOWER() is ASCII-only and
    must not stand in for casefold().
    """
    rows = db.imports.twin_rows_by_size(key[1])
    return [
        r for r in rows
        if stored_metadata_key(r["filename"], r["file_size"], r["timestamp"]) == key
    ]


def _hash_twin_rows(db: Database, file_hash: str) -> list[sqlite3.Row]:
    return db.imports.twin_rows_by_hash(file_hash)


def _likely_twin_rows(
    db: Database, token: DupToken, source_file: Path, path_under_source: Callable[[str], bool],
) -> list[sqlite3.Row]:
    """Return live off-source catalog twins that plausibly back ``token``.

    This is the intentionally fast duplicate mode: the checker has already
    matched filename + byte size + capture time (or, for metadata-poor files,
    a stored hash). We only stat the proposed archive twin to make sure it
    still exists off the card at the expected size; we do not read either
    complete file. Callers must report the resulting skip as unverified.
    """
    rows = (
        _hash_twin_rows(db, token[1])
        if token[0] == "hash"
        else _key_twin_rows(db, token[1])
    )
    try:
        source_size = os.path.getsize(str(source_file))
    except OSError:
        return []
    likely = []
    for row in rows:
        twin_path = os.path.join(row["folder_path"], row["filename"])
        if path_under_source(twin_path):
            continue
        try:
            if os.path.getsize(twin_path) != source_size:
                continue
        except OSError:
            continue
        likely.append(row)
    return likely


def _linkable_twin_dirs(rows: Iterable[sqlite3.Row], under_destination: Callable[[str], bool]) -> set[str]:
    """Destination-scoped folders holding a duplicate's cataloged twin.

    Only folders under the import destination are linked after a
    duplicate skip (a twin in some other library root is none of this
    import's business). Mirrors ingest()'s dup_token_folders guards:
    path under destination and still a real directory on disk. Status
    ``ok``/``partial`` is trusted as-is; ``missing`` is accepted too when
    the path is still a real directory (a reattached archive drive whose
    row hasn't been refreshed yet), and ``run_import_job`` promotes it
    to ``ok`` as part of the direct link — otherwise a duplicate-only
    batch that matches a missing-marked twin folder would drop it from
    ``dup_dirs``, safe_to_format could go green, and the imported
    duplicates would stay filtered out of workspace queries.

    ``under_destination(path)`` compares resolved/case-normalized paths
    (built in ``run_import_job`` from the destination's own filesystem
    semantics). A lexical prefix check would drop a twin folder when the
    destination is a symlink to the twin's on-disk archive root, or
    spelled with different case on a case-insensitive mount — dropping
    the twin means the direct workspace link never runs and the imported
    duplicate stays filtered out of the active workspace while
    safe_to_format still flips green. See PR #1107 review.
    """
    dirs = set()
    for r in rows:
        folder_path = r["folder_path"]
        if r["folder_status"] not in ("ok", "partial", "missing"):
            continue
        if not under_destination(folder_path):
            continue
        if not os.path.isdir(folder_path):
            continue
        dirs.add(folder_path)
    return dirs


def _link_duplicate_twin_dirs(
    db: Database, workspace_id: int, dup_dirs: Iterable[str],
) -> tuple[set[str], dict[str, str]]:
    """Link cataloged duplicate folders without walking archive storage.

    ``_linkable_twin_dirs`` has already proved each path is a cataloged
    folder under the destination and is currently a real directory. A
    filesystem scan adds no evidence about the duplicate bytes themselves;
    it only used to be an indirect way to create ``workspace_folders`` rows.
    Doing that directly keeps duplicate-only imports independent of SMB
    directory-enumeration latency.

    A reattached folder whose stale status is ``missing`` is promoted after
    the existence check. ``partial`` is intentionally preserved: only an
    explicit successful rescan may claim that an incomplete scan is repaired.
    Returns ``(linked_paths, failures)`` so callers can keep
    ``safe_to_format`` honest if the database link itself fails.
    """
    linked = set()
    failures = {}
    for folder_path in sorted(dup_dirs):
        try:
            folder_row = db.imports.folder_status_row(folder_path)
            if folder_row is None:
                raise RuntimeError("folder row not found")
            if folder_row["status"] == "missing":
                db.imports.promote_missing_folder(folder_row["id"])
            workspace_folders.add_workspace_folder(
                db, workspace_id, folder_row["id"], is_root=True,
            )
        except Exception as exc:  # broad: per-folder boundary; the failure is recorded and the other folders still link
            db.rollback()
            failures[folder_path] = str(exc)
            log.exception(
                "Linking duplicate-matched folder failed: %s", folder_path,
            )
            continue
        linked.add(folder_path)
        _invalidate_new_images(db, folder_path)
    return linked, failures
