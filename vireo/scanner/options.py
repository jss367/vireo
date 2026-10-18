"""The arguments ``scan()`` takes besides the root and the database.

``ScanOptions`` (what to scan and what the scan may write), ``ScanHooks``
(the callbacks it reports through) and ``ScanControl`` (the cancel and
pause probes). Each field documents itself.
"""

import os
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScanOptions:
    """What one ``scan()`` call covers and how it treats what it finds.

    Plain values only: scope (which files), catalog policy (what may be
    written) and the data directories the post-catalog passes use.
    """

    incremental: bool = False
    """If True, reuse complete records with matching file size and mtime.
    This does not verify byte identity; use a full scan to detect
    replacements that preserve both signals."""

    repair_missing_metadata: bool = False
    """In incremental mode, force rows whose ExifTool payload is NULL
    through metadata extraction even when a fallback timestamp was
    available during the original scan."""

    extract_full_metadata: bool = True
    """If True, store full ExifTool JSON in exif_data column."""

    skip_paths: Collection[str] | None = None
    """Optional set of absolute path strings to exclude from scanning."""

    recursive: bool = True
    """If True (default), scan subfolders; if False, only scan root
    directory."""

    restrict_dirs: Iterable | None = None
    """Optional list of directory paths to scan instead of the full tree.
    When provided, only files in these directories are discovered
    (non-recursively), but ``root`` is still used as the folder hierarchy
    root so parent links are preserved correctly."""

    restrict_files: Iterable | None = None
    """Optional iterable of absolute file paths. When provided alongside
    ``restrict_dirs``, only files whose path is in this set are discovered
    — untracked files in the same directory are ignored."""

    discovered_files: Iterable | None = None
    """Optional frozen iterable of paths from a preceding discovery pass.
    When supplied, scan skips its filesystem walk and processes exactly
    this manifest. Import-in-place uses this to establish a stable
    all-source progress denominator before work."""

    register_restrict_dirs_as_roots: bool = True
    """Restricted scans traditionally treat each explicit directory as a
    newly adopted workspace root. Set False when the restricted files
    already live below registered roots (for example a new-images snapshot
    or metadata repair): discovered descendants are linked to the
    workspace but are not promoted to additional roots."""

    allow_photo_inserts: bool = True
    """When False, update existing photo rows only. Files without an
    existing ``photos`` row are skipped. This gives Process metadata
    repair a mechanically enforced no-admission contract while retaining
    the shared metadata refresh code."""

    vireo_dir: str | os.PathLike | None = None
    """Optional path to the vireo data directory (e.g. ``~/.vireo``). When
    provided, working copies are extracted for RAW photos after companion
    pairing, and derived-cache invalidation fires on content-changed
    photos."""

    thumb_cache_dir: str | os.PathLike | None = None
    """Optional override for the thumbnail cache directory.
    ``--thumb-dir`` is independently configurable and can point anywhere —
    defaulting to ``vireo_dir/thumbnails`` silently misses the real cache
    when those diverge. Callers with the configured value (Flask routes,
    audit entry points) should pass it. When omitted, falls back to
    ``vireo_dir/thumbnails``."""

    skip_working_copies: bool = False
    """If True, suppress the end-of-scan ``_extract_working_copies`` pass
    while still running ``_pair_raw_jpeg_companions`` (with cache context)
    and ``_invalidate_derived_caches`` on content changes. Used by the
    per-batch import scan: the import job runs one deferred end-of-run
    extraction pass over all touched folders (a per-batch pass would race
    RAW+JPEG pairing across batch boundaries), but pairing itself still
    needs ``vireo_dir`` / ``thumb_cache_dir`` to move local-mask snapshots
    when a newly imported RAW pairs with an already-cataloged JPEG that
    has an edit recipe. Passing ``vireo_dir=None`` to suppress extraction
    would also silently drop those masks. See PR #1107 review."""


@dataclass(frozen=True)
class ScanHooks:
    """Callbacks ``scan()`` invokes to report what it is doing."""

    progress: Callable[..., Any] | None = None
    """Optional callable(current, total) for progress reporting."""

    status: Callable[..., Any] | None = None
    """Optional callable(message) for phase status updates. Callers may
    also accept keyword-only ``phase_current``, ``phase_total``, and
    ``phase_label`` for sub-phase progress."""

    photo: Callable[..., Any] | None = None
    """Optional callable(photo_id, path_str) called after each photo is
    committed. A JPEG the scan pairs with its RAW is reported under the
    RAW's id; it is never inserted as a photo first."""

    photo_merged: Callable[..., Any] | None = None
    """Optional callable(old_id, new_id, path_str) called after the
    end-of-scan pairing pass merges an existing JPEG photo into the RAW
    row ``new_id`` at ``path_str``. Called for every merge that pass
    commits, which can include pairs elsewhere in the catalog; a caller
    holding ``old_id`` (from ``photo``, this scan or an earlier one) must
    replace it, because the id no longer names a photo and SQLite can give
    it to the next insert. If the surviving RAW was deleted or replaced
    before publication, new_id and path_str are None: discard old_id
    without adopting a missing or unrelated replacement row."""

    permission_error: Callable[..., Any] | None = None
    """Optional callable(path_str) invoked once per directory the kernel
    refuses to enumerate (EPERM from macOS TCC, EACCES from POSIX mode
    bits). The scan continues; accessible siblings are still discovered.
    Without this hook, a denied subdir would silently produce zero hits —
    a "Found 0 images" black box from the user's perspective."""


@dataclass(frozen=True)
class ScanControl:
    """The caller's cancel and pause probes.

    Kept apart from ``ScanHooks`` because the scanner polls these to
    decide whether to stop, and the three must be wired as a set: which
    probe may park, and which may be called while CPU permits are held,
    is the contract described on each field.
    """

    cancel_check: Callable[[], Any] | None = None
    """Optional callable returning truthy when the caller wants scanning
    to stop promptly. When set, scan raises ScanCancelled at cancellation
    checkpoints. This probe MAY park (pipeline callers wire it to
    ``_pause_checkpoint`` which blocks on Pause); it is only called
    between leases, never inside ``_claim_worker_count``."""

    pause_check: Callable[[], Any] | None = None
    """Optional NON-parking probe returning truthy when a pause is
    pending. Checked at every inside-lease boundary so the scanner can
    raise ``_ScanPauseRequested`` and unwind the lease before the caller's
    parking ``cancel_check`` runs."""

    cancel_only_check: Callable[[], Any] | None = None
    """Optional NON-parking probe returning truthy when the job is
    cancelled (but NOT for pause — that is ``pause_check``'s job). Used
    inside the lease to detect cancellation without triggering the parking
    cancel path. Without this, the pause_check/cancel_check ordering has a
    race window: pause set between the pause probe and the (parking)
    cancel probe would park the job while still holding CPU permits and
    the process pool. Callers should pass ``runner.cancellation_requested``
    when they have a runner. When omitted, the inside-lease boundaries
    still call the parking ``cancel_check`` — the race window described in
    that Codex finding remains open in that fallback path, but nothing
    regresses vs. the pre-fix behavior."""
