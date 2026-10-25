"""Which photos get a working copy, and the entry points that extract them."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from image_loader import (
    RAW_EXTENSIONS,
    is_excluded_scan_path,
)
from scanner.working_copy_batch import _FAILURE_RETRY_AFTER, _working_copy_quota_mb, _WorkingCopyBatch

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Sequence

    from db import Database
    from scanner.scan_types import (
        CheckCallback,
        ProgressCallback,
        SourcePaths,
        StatusCallback,
        StrPath,
        WorkingCopyScopeEntry,
    )


def _subtree_like_pattern(path: str, sep: str | None = None) -> str:
    """Build a SQLite LIKE parameter that matches ``path`` + any descendant.

    Intended for use with ``LIKE ? ESCAPE '\\'``. Escapes ``\\``, ``%``, and
    ``_`` inside the path and the separator, so literal wildcards in folder
    names don't leak into sibling matches and — critically on Windows — the
    trailing backslash separator doesn't turn the appended ``%`` into a
    literal character.

    Trailing separators on the input are collapsed to exactly one before the
    wildcard. Without this, ``"/photos/"`` and the filesystem root ``"/"``
    produce ``"//%"``, which matches nothing.
    """
    if sep is None:
        sep = os.sep

    while path.endswith(sep):
        path = path[: -len(sep)]

    def _escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    return _escape(path) + _escape(sep) + "%"


def _working_copy_candidate_predicate(wc_max_size: int | None, alias: str = "") -> tuple[str, list[object]]:
    """Build the WHERE-clause fragment selecting photos eligible for working-copy
    extraction, plus its bind parameters.

    Mirrors the candidate criteria inside ``_extract_working_copies`` so the
    backfill summary counts match the extractor's actual SELECT, including
    its exclusion of small JPEGs.

    Failure suppression has two escape hatches: a content change (mtime
    differs from the recorded ``working_copy_failed_mtime``) and a stale
    timestamp (the failure was recorded more than ``_FAILURE_RETRY_AFTER``
    ago). The latter prevents transient I/O / environment failures (e.g.
    external drive temporarily disconnected at startup) from gating retries
    forever for files whose source bytes haven't moved.

    ``alias`` is the table alias (e.g. ``"p"``) when the caller's query joins
    other tables; pass ``""`` when ``photos`` is unaliased.
    """
    p = (alias + ".") if alias else ""
    placeholders = ",".join("?" for _ in RAW_EXTENSIONS)
    params: list[object] = list(RAW_EXTENSIONS)
    jpeg_clause = ""
    if wc_max_size and wc_max_size > 0:
        jpeg_clause = (
            f" OR (LOWER({p}extension) IN ('.jpg', '.jpeg', 'jpg', 'jpeg')"
            f"     AND ({p}width > ? OR {p}height > ?))"
        )
        params.extend([wc_max_size, wc_max_size])
    where = (
        f"{p}working_copy_path IS NULL"
        f" AND ({p}extension IN ({placeholders}){jpeg_clause})"
        f" AND ({p}working_copy_evicted_mtime IS NULL"
        f"      OR ({p}file_mtime IS NOT NULL"
        f"          AND {p}working_copy_evicted_mtime != {p}file_mtime))"
        f" AND ({p}working_copy_failed_at IS NULL"
        f"      OR {p}working_copy_failed_mtime IS NULL"
        f"      OR {p}file_mtime IS NULL"
        f"      OR {p}working_copy_failed_mtime != {p}file_mtime"
        f"      OR datetime({p}working_copy_failed_at)"
        f"         < datetime('now', ?))"
    )
    params.append(_FAILURE_RETRY_AFTER)
    return where, params


def working_copy_backfill_candidate_count(db: Database) -> int:
    """Count photos that ``_extract_working_copies`` would actually process.

    Used by ``backfill_working_copies`` for accurate before/after reporting.
    """
    import config as cfg

    user_cfg = cfg.load()
    wc_max_size = user_cfg.get("working_copy_max_size", 4096)
    try:
        wc_cache_max_mb = int(
            user_cfg.get("working_copy_cache_max_mb", 20480)
        )
    except (TypeError, ValueError):
        wc_cache_max_mb = 20480
    if wc_cache_max_mb <= 0:
        return 0
    where, params = _working_copy_candidate_predicate(wc_max_size)
    return db.working_copies.candidate_count(where, params)


def _extract_working_copies(
    db: Database, vireo_dir: StrPath, progress_callback: ProgressCallback | None = None,
    status_callback: StatusCallback | None = None,
    scope: Sequence[WorkingCopyScopeEntry] | None = None, cancel_check: CheckCallback | None = None,
    source_paths: SourcePaths | None = None,
    failed_paths_sink: list[str] | None = None,
) -> list[str]:
    """Extract working copies for all RAW photos missing one.

    Returns the cataloged paths of the photos whose extraction failed in this
    pass (each is also marked failed, see below), so a caller can name them.

    For each RAW photo without a working_copy_path, extract a JPEG working
    copy into ``<vireo_dir>/working/<photo_id>.jpg``.  When the photo has a
    companion JPEG (RAW+JPEG pair), the companion is used as the extraction
    source because the in-camera JPEG is higher quality than extracting from
    the RAW.

    Rows that previously failed extraction (``working_copy_failed_at`` set)
    are skipped unless ``file_mtime`` differs from the recorded
    ``working_copy_failed_mtime`` — a user-replaced file gets a fresh
    attempt; a permanently-broken file is not retried every pass.

    ``scope`` restricts which folders are considered:
      * ``None`` (default) — library-wide backfill (every missing WC).
      * list/tuple of entries — only folders matching an entry are eligible.
        Each entry is either:
          - a path string → matches the folder and every descendant (subtree);
          - a ``(path, "exact")`` tuple → matches the folder only;
          - a ``(path, "subtree")`` tuple → explicit form of the string case.
      * empty list/tuple — no-op (used by callers that want an explicit
        "scan matched nothing" signal instead of backfilling everything).

    ``progress_callback(current, total)`` is invoked once per processed row
    so long-running backfills can stream progress to the UI.

    ``cancel_check()`` is polled before each row; returning truthy aborts the
    loop cleanly with whatever was already committed.

    ``source_paths`` optionally maps a photo's cataloged absolute path to an
    ``(alternate_path, expected_size, expected_mtime_ns)`` tuple. The import
    job passes its card->archive mapping (with size + mtime captured at
    copy time) so extraction reads the fast local card instead of re-
    reading the just-written archive copy (which may live on a slow
    network volume). An override is used only when the file at
    ``alternate_path`` currently has the exact size AND mtime captured at
    copy time — anything else (rewritten card, remounted different card,
    unmounted card, coincidental same-size collision) falls back to the
    verified archive path. Applies to both the primary and the companion
    lookup; paths absent from the map read from the catalog location as
    usual.

    ``failed_paths_sink``, when supplied, is the list the batch appends
    cataloged paths to as it marks each per-photo failure. On the normal
    return path it is the same list the function returns. A caller that
    wraps this call in a try/except reads the sink after an exception
    propagates and still has the paths of photos the pass had already
    marked failed before it stopped.
    """
    import config as cfg

    if scope is not None and len(scope) == 0:
        return []

    user_cfg = cfg.load()
    wc_max_size = user_cfg.get("working_copy_max_size", 4096)
    wc_quality = user_cfg.get("working_copy_quality", 92)

    # A zero-byte quota is an explicit "keep no working copies" setting.
    # Skip RAW decoding entirely instead of generating a JPEG only for the
    # quota pass below to delete it immediately.
    wc_cache_max_mb = _working_copy_quota_mb(lambda: user_cfg)
    if wc_cache_max_mb <= 0:
        return []

    rows = _working_copy_candidate_rows(db, wc_max_size, scope)
    if not rows:
        return []

    batch = _WorkingCopyBatch(
        db, vireo_dir, rows,
        wc_max_size=wc_max_size,
        wc_quality=wc_quality,
        quota_mb=wc_cache_max_mb,
        scope=scope,
        source_paths=source_paths,
        cancel_check=cancel_check,
        progress_callback=progress_callback,
        status_callback=status_callback,
    )
    if failed_paths_sink is not None:
        # Share the caller's list so a mid-pass crash still surfaces the
        # photos the batch had already committed as failed: the batch
        # appends to this same object, and the caller reads it even on
        # the exception path.
        batch.failed_paths = failed_paths_sink
    batch.run()
    return batch.failed_paths


def import_working_copy_notice(failed_paths: Sequence[str], pass_error: str | None) -> str | None:
    """What an import tells the user about working copies it could not make.

    ``failed_paths`` are the photos the pass marked failed: the scanner
    skips those until 24 hours have passed or the file changes (see
    ``_working_copy_candidate_predicate``). ``pass_error`` is the error that
    stopped the whole pass; the photos it never reached carry no failure
    marker, so the next scan of their folder picks them up. None when there
    is nothing to say. Not an import failure: the originals were imported.
    """
    parts = []
    if failed_paths:
        n = len(failed_paths)
        if n == 1:
            parts.append(
                "Vireo could not create a working copy for 1 photo. The "
                "original was imported; only its working copy is missing. "
                "Vireo tries again the next time it scans the folder, once 24 "
                "hours have passed or sooner if the file changes."
            )
        else:
            parts.append(
                f"Vireo could not create a working copy for {n:,} photos. The "
                "originals were imported; only the working copies are missing. "
                "Vireo tries again the next time it scans their folders, once "
                "24 hours have passed or sooner if a file changes."
            )
    if pass_error:
        parts.append(
            f"Vireo stopped creating working copies after an error ({pass_error}), "
            "so some photos from this import may not have one yet. The "
            "originals were imported. Vireo creates the missing working copies "
            "the next time it scans their folders."
        )
    return " ".join(parts) or None


def _working_copy_scope_clause(db: Database, scope: Iterable[WorkingCopyScopeEntry]) -> str:
    """Load ``scope`` into a TEMP table; return the SQL clause matching it.

    Each scope folder is matched under the spelling a scan of it catalogs
    (``file_identity.catalog_scan_roots``), not the one the caller holds: an
    import reached through a symlink scopes ``/tmp/card`` while the scan
    filed the photos under ``/private/tmp/card``.
    """
    from file_identity import catalog_scan_roots

    entries = [
        entry if isinstance(entry, tuple) else (entry, "subtree")
        for entry in scope
    ]
    paths = catalog_scan_roots(db, [path for path, _mode in entries])
    scope_rows = []
    for path, (_supplied, mode) in zip(paths, entries, strict=True):
        if mode == "exact":
            scope_rows.append((path, mode, None))
        else:
            # Subtree match. The LIKE pattern needs to escape `_`, `%`,
            # and the escape char itself — both in the path and in the
            # separator — so literal wildcards in folder names cannot
            # leak into siblings.
            scope_rows.append(
                (path, "subtree", _subtree_like_pattern(path)),
            )

    # The TEMP table and why the scope lives there instead of in the WHERE
    # clause: ``WorkingCopyRepository.load_scope``.
    return db.working_copies.load_scope(scope_rows)


def _working_copy_candidate_rows(
    db: Database, wc_max_size: int | None, scope: Iterable[WorkingCopyScopeEntry] | None,
) -> list[sqlite3.Row]:
    """Snapshot the rows this pass should extract, newest import first."""
    # Candidate criteria (NULL working_copy_path + RAW or oversized JPEG +
    # not blocked by a stale failure marker) is shared with summary counts
    # via ``_working_copy_candidate_predicate`` so the two stay in sync.
    candidate_where, params = _working_copy_candidate_predicate(
        wc_max_size, alias="p"
    )
    scope_clause = ""
    if scope is not None:
        scope_clause = _working_copy_scope_clause(db, scope)

    # Newest import first (``WorkingCopyRepository.candidate_rows`` says
    # why).
    rows = db.working_copies.candidate_rows(candidate_where, scope_clause, params)

    # Revalidate cataloged paths at execution time. A user-selected source can
    # be replaced with a symlink into a protected macOS library after the API
    # request was validated but before this deferred extraction pass begins.
    # Filtering here keeps every caller from following stale folder rows into
    # another app's managed bundle.
    return [
        row for row in rows
        if not is_excluded_scan_path(row["folder_path"])
    ]


def backfill_working_copies(
    db: Database, vireo_dir: StrPath, progress_callback: ProgressCallback | None = None,
    status_callback: StatusCallback | None = None, cancel_check: CheckCallback | None = None,
) -> dict[str, int]:
    """Library-wide backfill of missing working copies.

    Convenience wrapper around ``_extract_working_copies`` with no folder
    scope, for explicit calls covering photos that never went through
    ``scan(..., vireo_dir=...)`` (e.g. legacy rows from
    before working-copy generation existed) or whose previous extraction
    failed against an older mtime.

    Returns a dict with ``processed`` (rows whose status changed,
    success+failure) so callers can summarize the run. Sequential by
    design: the bottleneck is a slow external disk where parallel reads
    thrash. If profiling later disagrees, swap in a worker pool here.
    """
    before_pending = working_copy_backfill_candidate_count(db)

    _extract_working_copies(
        db, vireo_dir,
        progress_callback=progress_callback,
        status_callback=status_callback,
        scope=None,
        cancel_check=cancel_check,
    )

    after_pending = working_copy_backfill_candidate_count(db)

    succeeded = db.working_copies.count_with_working_copy()

    return {
        "candidates": int(before_pending),
        "remaining": int(after_pending),
        "with_working_copy": int(succeeded),
    }
