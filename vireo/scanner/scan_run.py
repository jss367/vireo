"""``scan()`` and ``_ScanRun``: discovery, metadata, hashing and the per-file catalog pass.

The SQL lives in ``repositories/scanning.py`` (``db.scanning``).
"""

import contextlib
import errno
import json
import logging
import multiprocessing
import os
import time
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from dataclasses import dataclass
from pathlib import Path

from image_loader import (
    RAW_EXTENSIONS,
    SUPPORTED_EXTENSIONS,
    ScanCancelled,
    is_excluded_scan_path,
    safe_iter_dir,
    safe_scan_walk,
)
from metadata import (
    EXIF_SUMMARY_COLUMNS,
    exif_summary_columns,
    extract_metadata,
)
from PIL import Image
from preview_cache import (
    RecycledIdIndex,
    catalog_masks_directory,
    purge_cached_files_for_recycled_id,
)
from resource_ledger import suspend_resource_wait_timing
from scanner.cache_invalidation import _invalidate_derived_caches, _sweep_untracked_previews_for_photos
from scanner.companions import (
    _COMPANION_JPEG_EXTENSIONS,
    _COMPANION_RAW_EXTENSIONS,
    _companion_bytes_unchanged,
    _companion_stat_unchanged,
    _companions_compatible,
    _defer_primary_display_cache_invalidation,
    _fill_primary_metadata_gaps,
    _KnownCompanions,
    _pair_raw_jpeg_companions,
    _pairing_rows_for_stem,
    _pick_compatible_raw_jpeg_pairs,
    _read_metadata_transfer_rows,
    _run_post_commit_fs_actions,
)
from scanner.exif_fields import _extract_dimensions, _extract_timestamp
from scanner.hashing import (
    _SCAN_MP_METHOD,
    EMPTY_FILE_SHA256,
    _claim_worker_count,
    _compute_file_features,
    _ScanPauseRequested,
)
from scanner.keywords import _import_embedded_keywords_for_photo, _import_keywords_for_photo
from scanner.options import ScanControl, ScanHooks, ScanOptions
from scanner.status import _call_status_callback, _status_callback_supports_phase
from scanner.working_copies import _extract_working_copies

log = logging.getLogger(__name__)


_EMPTY_SCAN_COUNTS = {
    "discovered": 0, "indexed": 0, "vanished": 0, "skipped_uncataloged": 0,
    "merged_companions": 0, "known_companions": 0,
}


def _incremental_photo_index(db, image_files):
    """Load scan baselines for the requested paths across all workspaces.

    Indexed folder/filename lookups avoid repeatedly reading the entire
    catalog for every source in a multi-folder import. Bound each query
    below SQLite's parameter limit, and keep EXIF payloads out of memory.
    """
    result = {}
    for start in range(0, len(image_files), 200):
        batch = image_files[start:start + 200]
        rows = db.scanning.incremental_baseline_rows(
            [(str(path.parent), path.name) for path in batch],
        )
        for row in rows:
            result[os.path.join(row["folder_path"], row["filename"])] = row
    return result


def scan(root, db, *, options=None, hooks=None, control=None, counts=None,
         reported_photo_identities=None):
    """Walk a folder tree, discover photos, read metadata, populate database.

    Args:
        root: path to the root folder to scan
        db: Database instance
        options: ``ScanOptions`` — scope and catalog policy (defaults when
            omitted).
        hooks: ``ScanHooks`` — progress, status and per-photo callbacks.
        control: ``ScanControl`` — the cancel and pause probes.
        counts: optional dict the scan fills in as it runs, with the same
            keys it returns. Because scan() commits incrementally and can
            raise after many photos have landed (cancellation, a post-loop
            pass, a DB error), the return value alone would force callers
            to report zero for a run that really did catalog thousands.
            Pass a dict here to read accurate counts on any exit path.
        reported_photo_identities: optional dict shared across all scanner
            calls feeding one receiver. Stores the catalog identity reported
            for each id, so a later root cannot substitute an unrelated pair
            for a deleted photo reported by an earlier root.

    Returns:
        dict with ``discovered`` (files the walk turned up), ``indexed``
        (files that ended this run with a catalog row — including ones an
        incremental scan revalidated without changing, so this is "rows
        this run vouched for", not "rows newly created"), ``vanished``
        (discovered but gone by the time we stat'd them, the shape a
        network share dropping mid-scan takes), ``skipped_uncataloged``
        (update-only scans declining to insert), and
        ``merged_companions`` (JPEGs folded into a same-basename RAW's
        row, which are discounted from ``indexed`` because they stop
        being photos of their own), and ``known_companions`` (JPEGs the
        catalog already holds as their RAW's companion, found unchanged
        and left in place rather than re-imported and merged again).
        Build user-facing counts from ``indexed``, never from the progress
        counter — progress advances for skipped files too.
    """
    options = ScanOptions() if options is None else options
    hooks = ScanHooks() if hooks is None else hooks
    control = ScanControl() if control is None else control
    # Same object the caller passed (so it can read counts after an
    # exception) or a fresh one; either way start from a known shape.
    counts = {} if counts is None else counts
    counts.update(_EMPTY_SCAN_COUNTS)
    status_supports_phase = _status_callback_supports_phase(hooks.status)
    resolved = _resolve_scan_root(
        root, db, options.restrict_dirs, options.restrict_files,
        options.skip_paths, options.discovered_files,
    )
    if resolved is None:
        return counts
    root_path, restrict_dirs, restrict_files, skip_paths, frozen_files = resolved
    from file_identity import catalog_folder_aliases, catalog_folder_path

    _ScanRun(
        root, root_path, db, counts, frozen_files,
        catalog_folder_path=catalog_folder_path,
        catalog_folder_aliases=catalog_folder_aliases,
        options=options,
        hooks=hooks,
        control=control,
        status_supports_phase=status_supports_phase,
        skip_paths=skip_paths,
        restrict_dirs=restrict_dirs,
        restrict_files=restrict_files,
        reported_photo_identities=reported_photo_identities,
    ).run()
    return counts


def _resolve_scan_root(root, db, restrict_dirs, restrict_files, skip_paths,
                       discovered_files):
    """The cataloged root and the caller's inputs rebased under it.

    Returns ``(root_path, restrict_dirs, restrict_files, skip_paths,
    frozen_files)``, or ``None`` when the root must not be scanned (an
    other-app data bundle, or a missing root without a frozen manifest).
    """
    root_path = Path(root)
    # Don't open the root at all if the root is, or sits inside, an
    # other-app data bundle. prune_scan_dirs below only filters
    # *children*, so a root of e.g.
    # ``~/Pictures/Photos Library.photoslibrary`` — or a directly
    # selected/stale subfolder like ``.../Photos Library.photoslibrary/originals``
    # — would still trigger the macOS "access data from other apps" TCC
    # prompt this guard exists to avoid. Check every ancestor, not just
    # the leaf name. This must run BEFORE ``root_path.is_dir()``: is_dir
    # follows symlinks and stat's the target, so for a directly selected
    # bundle (or a symlink to one), the existence test alone is enough
    # to trip the TCC prompt — mirroring the restrict_dirs branch below.
    # Both bail-outs below return None, and ``scan()`` returns the same
    # counts shape as a completed scan (all zeros) rather than None, so
    # callers can total up ``indexed`` across roots without special-casing
    # the roots that never ran. See the summary block in
    # ``_ScanRun._log_summary``.
    if is_excluded_scan_path(root_path):
        log.info(
            "Skipping other-app data bundle as scan root: %s", root_path,
        )
        return None
    from file_identity import catalog_scan_root

    original_root = os.path.abspath(root_path)
    root_path = Path(catalog_scan_root(db, original_root))
    if str(root_path) != original_root:
        restrict_dirs, restrict_files, skip_paths, discovered_files = (
            _rebase_scan_inputs(
                original_root, root_path,
                restrict_dirs, restrict_files, skip_paths, discovered_files,
            )
        )
    # A frozen manifest may be any iterable, including a generator. Consume
    # it exactly once so the missing-root guard can distinguish an empty
    # manifest from promised work without exhausting the later work queue.
    frozen_files = (
        None if discovered_files is None else list(discovered_files)
    )
    if not root_path.is_dir():
        # A frozen manifest promised these files exist; the missing root
        # is a source-level failure the caller must see (an SD card
        # ejected between discovery and scan, or a network mount that
        # dropped). Silently returning zero counts would hide the loss
        # from ``root_errors``, so the multi-source coordinator would
        # advance its "Overall" denominator without ever processing this
        # source and mark the import successful on an incomplete set.
        # Without a manifest, missing roots stay a benign no-op — an
        # empty-manifest run wants the same shape a completed one has.
        if frozen_files:
            raise FileNotFoundError(
                errno.ENOENT,
                "scan root disappeared between discovery and scan",
                str(root_path),
            )
        log.warning("Root path does not exist or is not a directory: %s", root)
        return None
    return root_path, restrict_dirs, restrict_files, skip_paths, frozen_files


def _rebase_scan_inputs(original_root, root_path, restrict_dirs,
                        restrict_files, skip_paths, discovered_files):
    """Re-express the caller's paths under the cataloged root spelling."""
    def rebase_scan_path(path):
        path = os.path.abspath(path)
        try:
            if os.path.commonpath([original_root, path]) == original_root:
                return str(root_path / os.path.relpath(path, original_root))
        except ValueError:
            pass
        return path

    restrict_dirs = None if restrict_dirs is None else [rebase_scan_path(p) for p in restrict_dirs]
    restrict_files = None if restrict_files is None else [rebase_scan_path(p) for p in restrict_files]
    skip_paths = None if skip_paths is None else {rebase_scan_path(p) for p in skip_paths}
    discovered_files = None if discovered_files is None else [Path(rebase_scan_path(p)) for p in discovered_files]
    return restrict_dirs, restrict_files, skip_paths, discovered_files


@dataclass
class _FileMetadata:
    """What the main loop reads from one file's ExifTool payload."""

    file_meta: dict
    width: int | None
    height: int | None
    timestamp: str | None
    burst_id: str | None
    latitude: float | None
    longitude: float | None


@dataclass
class _IndexedFile:
    """One hashed file the main loop is cataloging."""

    image_path: Path
    folder_id: int
    file_size: int
    file_mtime: float
    file_hash: str | None
    phash: str | None
    xmp_path: Path
    xmp_mtime: float | None
    meta: _FileMetadata
    row_already_existed: bool
    prev_file_hash: str | None


class _CompanionAttachRefused(Exception):
    """Rolls back a row-less companion attach whose owner changed."""


def _companion_group_key(image_path):
    """``(directory, stem)``: the files pairing considers together."""
    return (str(image_path.parent), image_path.stem)


class _ScanRun:
    """One ``scan()`` past its early exits.

    Walks the phases in order — discovery, the incremental pre-pass,
    batched metadata extraction, parallel hashing feeding the per-file
    catalog loop, then companion pairing and working copies — sharing the
    work queue, folder cache and progress tally between them.
    """

    def __init__(
        self, root, root_path, db, counts, frozen_files, *,
        catalog_folder_path, catalog_folder_aliases,
        options, hooks, control, status_supports_phase,
        skip_paths, restrict_dirs, restrict_files,
        reported_photo_identities=None,
    ):
        self.root = root
        self.root_path = root_path
        self.db = db
        self.counts = counts
        self.frozen_files = frozen_files
        self.catalog_folder_path = catalog_folder_path
        self.catalog_folder_aliases = catalog_folder_aliases
        self.progress_callback = hooks.progress
        self.incremental = options.incremental
        self.extract_full_metadata = options.extract_full_metadata
        self.photo_callback = hooks.photo
        self.photo_merged_callback = hooks.photo_merged
        self._reported_identities = (
            reported_photo_identities if reported_photo_identities is not None else {}
        )
        # The rebased spellings ``_resolve_scan_root`` returned, not the
        # caller's ``options`` values.
        self.skip_paths = skip_paths
        self.status_callback = hooks.status
        self.status_supports_phase = status_supports_phase
        self.recursive = options.recursive
        self.restrict_dirs = restrict_dirs
        self.restrict_files = restrict_files
        self.vireo_dir = options.vireo_dir
        self.thumb_cache_dir = options.thumb_cache_dir
        self.permission_error_callback = hooks.permission_error
        self.cancel_check = control.cancel_check
        self.pause_check = control.pause_check
        self.cancel_only_check = control.cancel_only_check
        self.skip_working_copies = options.skip_working_copies
        self.repair_missing_metadata = options.repair_missing_metadata
        self.register_restrict_dirs_as_roots = options.register_restrict_dirs_as_roots
        self.allow_photo_inserts = options.allow_photo_inserts

    def run(self):
        self._discover_files()
        self._count_discovered()
        self._load_catalog_state()
        self.files_to_process = self._triage_files()
        self.paths_to_extract = self._extract_metadata()
        self._index_files()
        self._finish_catalog()
        self._log_summary()

    def _check_cancelled(self):
        if self.cancel_check is None:
            return
        # ``_check_cancelled`` is threaded into ``ResourceLedger.acquire``
        # through ``_claim_worker_count`` as its cancellation probe. On a
        # standalone ``/api/jobs/scan`` or ``/api/jobs/import-photos`` job
        # the caller-supplied ``cancel_check`` is the runner's pause-aware
        # probe, so it parks for the whole pause; without the suspend
        # bracket, the ledger's active wait timer accumulates that idle
        # time as resource contention and inflates the persisted
        # ``resource_wait_seconds`` diagnostic by the full pause length.
        # The pipeline path already brackets its checkpoints with
        # ``suspend_resource_wait_timing()`` inside ``_pause_checkpoint``;
        # mirror that here so both call sites report contention honestly.
        # When there is no active wait (every non-``ledger.acquire`` call
        # site), the suspend context is a cheap no-op.
        with suspend_resource_wait_timing():
            cancelled = self.cancel_check()
        if cancelled:
            raise ScanCancelled("scan cancelled")

    def _emit_status(self, message, phase_current=None, phase_total=None, phase_label=None):
        if not self.status_callback:
            return
        _call_status_callback(
            self.status_callback,
            message,
            phase_current=phase_current,
            phase_total=phase_total,
            phase_label=phase_label,
            supports_phase=self.status_supports_phase,
        )

    def _skip_file(self, bucket):
        """Dispose of a file that will not be indexed, under ``bucket``."""
        self.processed_count += 1
        self.counts[bucket] += 1
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)

    # -- discovery ---------------------------------------------------

    def _discover_files(self):
        # Discover all image files (incremental enumeration for progress reporting)
        # unless the caller already froze an all-source manifest. Copy the input:
        # scan sorts its work queue and callers may retain their source mapping.
        log.info("Discovering files in %s ...", self.root)
        self._check_cancelled()
        if self.frozen_files is None and self.status_callback:
            self._emit_status("Discovering files...")
        self.excluded_frozen = 0
        if self.frozen_files is not None:
            self._filter_frozen_manifest()
        else:
            self.image_files = []

        # Tracks restrict_dirs entries that survive the bundle guard, so the
        # working-copy extraction pass below scopes its SQL query to the same
        # set the discovery loop actually visited. Without this, a stale folder
        # row inside an excluded bundle (carried over from before the guard)
        # would be re-touched by ``_extract_working_copies`` reading
        # ``folder_path/filename`` — re-tripping the macOS TCC prompt this guard
        # exists to avoid.
        self.effective_restrict_dirs = []
        if self.frozen_files is not None:
            # The manifest already applied the file filters. Retain only the
            # directory scope used by the later working-copy extraction pass;
            # do not enumerate those directories a second time.
            self.effective_restrict_dirs = [
                d for d in (self.restrict_dirs or [])
                if not is_excluded_scan_path(Path(d))
            ]
        elif self.restrict_dirs is not None:
            self._discover_restricted()
        elif self.recursive:
            self._discover_recursive()
        else:
            self._discover_flat()
        self.image_files.sort()
        self._check_cancelled()

    def _filter_frozen_manifest(self):
        # A frozen manifest was captured before any per-source scan began, so
        # a later source can wait minutes behind earlier ones. In that window
        # a nested child dir, mount, or symlink under this source may be
        # replaced with (or into) a macOS app-managed library — a swap the
        # app-level pre-scan mount-identity check cannot see, because it
        # only revalidates the source root. Rerun the bundle guard on every
        # frozen path here so the later ``image_path.stat()`` cannot follow
        # a replacement into ``Photos Library.photoslibrary`` (or a sibling
        # excluded bundle) and re-trip the TCC prompt this guard exists to
        # avoid — or catalog files from a substituted subtree.
        self.image_files = []
        for path in self.frozen_files:
            candidate = Path(path)
            if is_excluded_scan_path(candidate):
                self.excluded_frozen += 1
                continue
            self.image_files.append(candidate)
        if self.excluded_frozen:
            log.warning(
                "Frozen manifest for %s: %d path(s) now resolve into an "
                "excluded app-managed bundle and were skipped",
                self.root, self.excluded_frozen,
            )

    # os.walk + onerror, not Path.rglob: rglob silently skips any
    # subdir that raises during enumeration, so a TCC-denied folder
    # turns into "Found 0 images" with no signal that anything went
    # wrong. macOS TCC raises EPERM ("Operation not permitted");
    # POSIX mode-bit denials raise EACCES. Both must be reported,
    # not swallowed. Other OSError flavors are still surfaced via
    # log so we don't mask unexpected I/O problems. Defined here
    # (not nested in a branch) so the restrict_dirs path below can
    # also surface denials through the same callback.
    #
    # Partial-success (skip the denied dir, keep scanning siblings)
    # is *opt-in* via permission_error_callback. Without a callback,
    # re-raise the PermissionError: existing OSError-aware callers
    # (e.g. pipeline_job repair scan's `except (OSError, RuntimeError)`
    # unreachable counter) rely on the failure to surface, and silent
    # skipping would let a denied folder be reported as successfully
    # repaired. Callers that want to continue past denials must pass
    # the callback to acknowledge they've taken responsibility for
    # streaming the denial somewhere actionable.
    def _on_walk_error(self, err):
        if err.errno in (errno.EPERM, errno.EACCES):
            denied = err.filename or str(self.root_path)
            log.warning(
                "Permission denied enumerating %s — skipping. "
                "On macOS this usually means TCC (Privacy & Security "
                "→ Files and Folders / Removable Volumes) needs to "
                "grant Vireo access.", denied,
            )
            if self.permission_error_callback is not None:
                self.permission_error_callback(denied)
                return
            raise err
        else:
            log.warning("os.walk error at %s: %s", err.filename, err)

    def _discover_restricted(self):
        # Only enumerate files in the specified directories (non-recursive).
        # root is still used as the folder hierarchy root for _ensure_folder.
        restrict_files_set = set(self.restrict_files) if self.restrict_files is not None else None
        skip_paths = self.skip_paths
        image_files = self.image_files
        # Heartbeat counter, same interval as the recursive branch below.
        # A restricted dir is not necessarily small — the import job's
        # duplicate-folder link scan points this at archive day-folders
        # holding a whole card's worth of files, and on a network mount
        # that enumeration runs for minutes. Without the emit the caller
        # has nothing to show between "Discovering files..." and the
        # finished count.
        checked = 0
        for d in self.restrict_dirs:
            self._check_cancelled()
            dp = Path(d)
            # The outer ``is_excluded_scan_path(root_path)`` guard above only
            # covers ``root``. restrict_dirs entries can independently point
            # into an other-app data bundle — e.g. a stale folder row from
            # before this guard, or a duplicate the caller built from
            # workspace_folders pointing at
            # ``~/Pictures/Photos Library.photoslibrary/originals``. Calling
            # ``dp.is_dir()`` / ``dp.iterdir()`` on that subtree would still
            # trip the macOS "access data from other apps" TCC prompt this
            # guard exists to avoid. Reject before any filesystem access.
            if is_excluded_scan_path(dp):
                log.info(
                    "Skipping other-app data bundle in restrict_dirs: %s", dp,
                )
                continue
            self.effective_restrict_dirs.append(d)
            if dp.is_dir():
                # safe_iter_dir mirrors iterdir() but drops excluded
                # bundle children (direct ``Photos Library.photoslibrary``
                # entries or symlinks pointing into one) before the
                # is_file()/suffix filter below would stat them — that
                # stat alone would re-trip the macOS "access data from
                # other apps" TCC prompt this guard exists to avoid.
                # Permission denials route through _on_walk_error: when
                # ``permission_error_callback`` is registered the helper
                # logs + invokes it and returns, so the generator yields
                # nothing and the for-loop falls through to the next
                # ``d``. Without a callback, _on_walk_error re-raises so
                # callers like pipeline_job's repair scan (``except
                # (OSError, RuntimeError)``) keep their loud failure
                # semantics — we deliberately don't catch that raise.
                #
                # Consume the generator directly rather than materializing
                # with ``list(...)``: on a slow network mount the scandir
                # walk that backs safe_iter_dir takes minutes for a full
                # day-folder, and materializing here would delay every
                # heartbeat below until after that wait — the same silent
                # hang the heartbeat exists to break (PR #1385 Codex /
                # CodeRabbit review).
                for f in safe_iter_dir(str(dp), onerror=self._on_walk_error):
                    self._check_cancelled()
                    checked += 1
                    if checked % 500 == 0 and self.status_callback:
                        self._emit_status(
                            f"Discovering files... ({len(image_files)} found)"
                        )
                    # Filter restricted imports before statting a sibling:
                    # an unreadable collision candidate is unrelated to
                    # the successfully landed files we are cataloging.
                    if (f.suffix.lower() in SUPPORTED_EXTENSIONS
                            and not f.name.startswith(".")
                            and (skip_paths is None or str(f) not in skip_paths)
                            and (restrict_files_set is None
                                 or str(f) in restrict_files_set)
                            and f.is_file()):
                        image_files.append(f)

    def _discover_recursive(self):
        skip_paths = self.skip_paths
        image_files = self.image_files
        checked = 0
        # safe_scan_walk replaces os.walk + prune_scan_dirs. It excludes
        # other-app data bundles (e.g. "Photos Library.photoslibrary"
        # sitting in ~/Pictures) without ever stat-following a symlink
        # to one — os.walk's classification call follows symlinks and
        # would re-trip the macOS "access data from other apps" TCC
        # prompt for a child like ``LibraryAlias -> Photos
        # Library.photoslibrary`` before prune_scan_dirs could reject it.
        # Pass cancel_check into safe_scan_walk so it polls between
        # scandir entries as well: a single very large directory (a
        # media dump with 1M+ files) can otherwise consume the whole
        # ``os.scandir`` loop before yielding, leaving the per-yield
        # ``_check_cancelled()`` below unreachable until the walker
        # finishes filling its dirs/nondirs lists.
        for dirpath, _dirnames, filenames in safe_scan_walk(
            str(self.root_path), onerror=self._on_walk_error,
            cancel_check=self.cancel_check,
        ):
            self._check_cancelled()
            for name in filenames:
                checked += 1
                if checked % 100 == 0:
                    self._check_cancelled()
                if checked % 500 == 0 and self.status_callback:
                    self._emit_status(
                        f"Discovering files... ({len(image_files)} found)"
                    )
                ext = os.path.splitext(name)[1].lower()
                if (ext in SUPPORTED_EXTENSIONS
                        and not name.startswith(".")):
                    full = os.path.join(dirpath, name)
                    if skip_paths is not None and full in skip_paths:
                        continue
                    # os.walk includes broken symlinks in `filenames`,
                    # but the pre-pass below calls image_path.stat()
                    # which would raise FileNotFoundError and abort
                    # the whole scan. The previous Path.rglob path
                    # filtered these via is_file(); preserve that.
                    # os.path.isfile follows symlinks and returns
                    # False (not raise) for dangling targets.
                    if not os.path.isfile(full):
                        continue
                    image_files.append(Path(full))

    def _discover_flat(self):
        skip_paths = self.skip_paths
        image_files = self.image_files
        # safe_iter_dir mirrors iterdir() but drops excluded bundle
        # children before the is_file() filter below would stat
        # them. A normal root like ~/Pictures can hold ``Photos
        # Library.photoslibrary`` (or a symlink to one) as a direct
        # child; a bare iterdir + is_file() would stat that bundle
        # and re-trip the macOS "access data from other apps" TCC
        # prompt this guard exists to avoid, even though the
        # extension filter would have rejected it afterwards.
        # Permission denials route through _on_walk_error (callback
        # → empty entries, no callback → re-raise) the same way the
        # restrict_dirs branch above does.
        entries = list(safe_iter_dir(str(self.root_path), onerror=self._on_walk_error))
        for checked, f in enumerate(entries, 1):
            if checked % 100 == 0:
                self._check_cancelled()
            if checked % 500 == 0 and self.status_callback:
                self._emit_status(
                    f"Discovering files... ({len(image_files)} found)"
                )
            if (f.is_file()
                    and f.suffix.lower() in SUPPORTED_EXTENSIONS
                    and not f.name.startswith(".")
                    and (skip_paths is None or str(f) not in skip_paths)):
                image_files.append(f)

    def _count_discovered(self):
        # Excluded frozen paths were part of the caller's promised manifest.
        # Account for them as vanished work so progress still reaches that
        # frozen denominator and import-in-place surfaces a partial-source
        # failure instead of silently succeeding on a smaller queue.
        self.total = len(self.image_files) + self.excluded_frozen
        self.counts["discovered"] = self.total
        self.counts["vanished"] = self.excluded_frozen
        log.info("Found %d images in %s", self.total, self.root)
        if self.progress_callback:
            self.progress_callback(self.excluded_frozen, self.total)

    # -- catalog state -----------------------------------------------

    def _load_catalog_state(self):
        db = self.db
        self.existing_by_path = (
            _incremental_photo_index(db, self.image_files)
            if self.incremental else {}
        )
        self.known_companions = _KnownCompanions(db)
        # Companion JPEGs the incremental pre-pass found unchanged by stat,
        # as ``(image_path, owner_id)``; settled once the pre-pass knows
        # which RAW rows it is re-reading (see ``_settle_stat_unchanged_companions``).
        self.stat_unchanged_companions = []
        # Cataloged rows the pre-pass sends for re-reading.
        self.reprocessed_row_ids = set()
        # ``owner_id -> (ownership, companion columns)`` for companions found unchanged
        # after their metadata was read. Populated as each companion is
        # processed; drained by ``_write_photo_columns`` when the owner is
        # re-read (so the gap fill lands in the same UPDATE as the
        # clear-to-NULL — see ``_write_photo_columns``), and by the
        # end-of-scan safety net ``_refill_owners_from_companions``.
        self._pending_companion_fills = {}
        self._init_collection_membership_cache()

        # Build folder cache: path -> folder_id
        self.folder_cache = {}
        self.folder_aliases = self.catalog_folder_aliases(db)

        # When the scan is restricted to specific subfolders, those subfolders —
        # not the broad scan root — are the user-facing workspace roots. A
        # templated copy-import lands files in ``<destination>/<template>/...``
        # dirs and passes those leaf dirs as ``restrict_dirs`` while keeping the
        # destination base as ``root`` only for parent-chain creation. Promoting
        # the base to a workspace root would make the new-images walk treat every
        # un-imported sibling under it as "new" (e.g. a whole archive of past
        # shoots sharing the destination). Mark the restricted dirs as roots
        # instead; the base stays linked but is_root=0. See
        # ``new_images.mapped_roots``. ``effective_restrict_dirs`` (bundle-filtered)
        # is the set actually enumerated, so root marking matches what was scanned.
        self._effective_restrict_paths = None
        self._restrict_root_paths = None
        if self.restrict_dirs is not None:
            self._effective_restrict_paths = {
                os.path.normpath(str(d)) for d in self.effective_restrict_dirs
            }
            self._restrict_root_paths = (
                self._effective_restrict_paths
                if self.register_restrict_dirs_as_roots else set()
            )

        # Track folders whose scan touched them (so we can flag them 'partial'
        # if anything between the pre-pass and scan completion dies midway) and
        # the outer scan scope as a fallback. The scope matters when
        # ``touched_folder_ids`` is empty — e.g. a pre-pass XMP commit that
        # aborts before the main loop has added any folder, or a successful
        # no-op incremental scan that processes zero files.
        self.touched_folder_ids = set()
        # Photo IDs whose derived caches were invalidated this scan. Collected
        # so the untracked-preview sweep can run once as a batch instead of
        # per-photo (avoids O(N × M) directory walks on large rescans).
        self.invalidated_photo_ids: set[int] = set()
        # Photo IDs that already own cached derivative files, for the
        # recycled-rowid check on insert. Snapshotted once (lazily, on the
        # first insert) for the same batching reason as the sweep above.
        self.recycled_id_index = RecycledIdIndex(
            self.thumb_cache_dir or (
                os.path.join(self.vireo_dir, "thumbnails")
                if self.vireo_dir else ""
            ),
            vireo_dir=self.vireo_dir,
            masks_dir=catalog_masks_directory(self.db._db_path),
        )
        self.scoped_paths = {str(self.root_path)}
        if self.restrict_dirs is not None:
            self.scoped_paths.update(str(d) for d in self.restrict_dirs)

    def _ensure_folder(self, folder_path):
        """Ensure a folder and all its parents exist in the DB. Returns folder_id."""
        db = self.db
        root_path = self.root_path
        folder_str = str(folder_path)
        if folder_str in self.folder_cache:
            return self.folder_cache[folder_str]

        parent_id = None
        if folder_path != root_path:
            parent_id = self._ensure_folder(folder_path.parent)

        if self._restrict_root_paths is not None:
            is_ws_root = os.path.normpath(folder_str) in self._restrict_root_paths
        else:
            is_ws_root = (folder_path == root_path)

        # In restricted mode, the scan root (``root_path``) and every
        # intermediate ancestor between it and ``restrict_dirs`` exist
        # only to satisfy the ``folders.parent_id`` chain — they are not
        # part of what the user asked to import. Linking them to the
        # workspace would fire ``_add_workspace_folder_no_commit`` and
        # its path-prefix subtree cascade, pulling every pre-existing
        # cataloged descendant of ``root_path`` (unrelated archive
        # subtrees from prior scans / other workspaces) into the active
        # workspace. Only the ``_restrict_root_paths`` themselves should
        # link. See PR #1107 review (line 1186).
        normalized_folder = os.path.normpath(folder_str)
        is_restrict_target = (
            self._effective_restrict_paths is not None
            and normalized_folder in self._effective_restrict_paths
        )
        link_to_ws = (
            self._effective_restrict_paths is None
            or (self.register_restrict_dirs_as_roots and is_restrict_target)
        )

        folder_id = db.add_folder(
            path=self.catalog_folder_path(
                db, folder_str, aliases=self.folder_aliases,
            ),
            name=folder_path.name,
            parent_id=parent_id,
            workspace_root=is_ws_root,
            link_to_workspace=link_to_ws,
        )
        if (
            is_restrict_target
            and not self.register_restrict_dirs_as_roots
            and db.active_workspace_id is not None
        ):
            # Snapshot imports and metadata repair select exact leaf folders
            # below an existing workspace root. Link only that leaf: the
            # regular subtree-linking API would also attach unrelated known
            # descendants that this restricted scan never touched.
            db.add_workspace_folder_exact(
                db.active_workspace_id, folder_id, is_root=False,
            )
        self.folder_cache[folder_str] = folder_id
        return folder_id

    def _update_folder_status(self, new_status, only_from_partial):
        """Stamp folders in the scan scope with ``new_status``.

        Applies to every folder matched by ``scoped_paths`` (outer roots)
        OR ``touched_folder_ids`` (folder rows the main loop has reached).
        When ``only_from_partial`` is True, restricts the UPDATE to rows
        already in ``'partial'`` — used on the success path so completed
        scans don't clobber ``'missing'`` or future statuses.
        """
        db = self.db
        scoped_paths = self.scoped_paths
        touched_folder_ids = self.touched_folder_ids
        db.scanning.set_folder_status(
            new_status, paths=scoped_paths, folder_ids=touched_folder_ids,
            only_from_partial=only_from_partial,
        )
        db.commit_with_retry()

    # -- pre-pass ----------------------------------------------------

    def _triage_files(self):
        """Return the files that need full processing."""
        # First pass: determine which files need full processing (for incremental mode).
        # Handle XMP-only changes inline; collect files needing metadata extraction.
        files_to_process = []
        self.processed_count = self.excluded_frozen
        # ``processed_count`` advances for every file the scan *disposes of*,
        # including ones it deliberately skips — that is what a progress bar
        # needs to reach 100%. It is NOT the number of photos indexed, and the
        # two diverge exactly when something is wrong (a share that unmounts
        # mid-scan makes every remaining file vanish). Every ``processed_count``
        # bump below is therefore paired with a bump of exactly one bucket in
        # ``counts``, so the summary reports what actually landed in the
        # catalog. Classifying at each site (rather than deriving one bucket by
        # subtraction) means a disposition added later has to state which
        # bucket it belongs to instead of silently defaulting to "indexed" —
        # the invariant check after the loop enforces it.
        #
        # The ids behind ``counts["indexed"]``. Needed because the companion
        # pairing pass below runs against the entire photos table, so its
        # merges must be intersected with what *this* invocation counted (see
        # ``_pair_raw_jpeg_companions``).
        self.indexed_photo_ids = set()
        # The identity history initialized by __init__ spans the receiver's
        # scanner calls. Do not reset it at this per-root phase boundary:
        # end-of-scan pairing may merge ids reported by an earlier root.
        try:
            self._register_scan_targets()
            for image_path in self.image_files:
                self._check_cancelled()
                if self._needs_processing(image_path):
                    files_to_process.append(image_path)
            if self._settle_stat_unchanged_companions(files_to_process):
                files_to_process.sort()
        except BaseException:
            # Pre-pass died (e.g. non-retryable DB error on an XMP commit).
            # Route through the same partial-status path as a main-loop failure
            # so users see the badge and can rescan.
            try:
                self.db.rollback()
            except Exception:
                log.exception("Rollback after pre-pass failure also failed")
            try:
                self._update_folder_status("partial", only_from_partial=False)
            except Exception:
                log.exception("Failed to flag folders partial after pre-pass failure")
            raise
        return files_to_process

    def _register_scan_targets(self):
        # Eagerly register the explicit scan targets so they end up linked
        # to the active workspace even when zero photos are inserted (e.g.
        # every file is in skip_paths because the photos are already in
        # the global photos table). Without this, importing a folder
        # whose contents are already known silently skipped the
        # workspace_folders link and the folder never became visible in
        # the active workspace. Inside the try so a DB failure here still
        # routes through the partial-status recovery path.
        self._ensure_folder(self.root_path)
        if self.restrict_dirs is not None:
            for d in self.restrict_dirs:
                dp = Path(d)
                # Same bundle guard as the discovery loop above — never
                # register or stat a path inside an other-app data bundle,
                # even when the caller stuffed one into ``restrict_dirs``.
                if is_excluded_scan_path(dp):
                    continue
                if dp.is_dir():
                    self._ensure_folder(dp)

    def _needs_processing(self, image_path):
        """``False`` when the pre-pass already disposed of ``image_path``."""
        try:
            stat = image_path.stat()
        except OSError:
            # File deleted/renamed between discovery and this pass —
            # skip it instead of aborting the whole scan (the discovery
            # walk has the same guard for broken symlinks).
            log.info("File vanished during scan, skipping: %s", image_path)
            self._skip_file("vanished")
            return False
        xmp_path = image_path.with_suffix(".xmp")
        try:
            xmp_mtime = xmp_path.stat().st_mtime
        except OSError:
            # Covers both "no sidecar" and a sidecar deleted between
            # exists() and stat() — same outcome either way.
            xmp_mtime = None

        if self.incremental:
            full_path_str = str(image_path)
            existing = self.existing_by_path.get(full_path_str)
            if existing:
                if self._reuse_existing_row(
                    existing, full_path_str, stat, xmp_path, xmp_mtime,
                ):
                    return False
                self.reprocessed_row_ids.add(existing["id"])
            else:
                known = self.known_companions.lookup_path(image_path)
                if known is not None and _companion_stat_unchanged(known, stat):
                    self.stat_unchanged_companions.append(
                        (image_path, known),
                    )
                    return False
        return True

    def _settle_stat_unchanged_companions(self, files_to_process):
        """Credit unchanged companions, or re-read them beside a re-read RAW.

        Re-reading a RAW rewrites its EXIF summary columns from the RAW
        alone, clearing the ones only the camera JPEG carries. Pairing
        filled those from the JPEG, so a companion whose RAW is being
        re-read is read too, and its columns refill the gaps (see
        ``_refill_owners_from_companions``). Returns whether any were
        added to ``files_to_process``.

        ``known`` was cached during ``_needs_processing``; between then
        and here another connection could have deleted the owner row and
        SQLite could have reused that ID for an unrelated photo. Lock
        the expected ownership (``folder_id`` + ``filename`` +
        ``companion_path``) before importing the sidecar or crediting
        the callback, so a stale cache can neither write this
        companion's XMP keywords into a replacement row nor report that
        replacement to the pipeline collection and thumbnail queue.
        """
        added = False
        for image_path, known in self.stat_unchanged_companions:
            owner_id = known["owner_id"]
            if owner_id in self.reprocessed_row_ids:
                files_to_process.append(image_path)
                added = True
            else:
                if not self._finish_known_companion(image_path, known):
                    # The cached owner vanished or changed. Hash and
                    # catalog the JPEG against fresh state rather than
                    # losing its disposition or crediting a recycled ID.
                    files_to_process.append(image_path)
                    added = True
        self.stat_unchanged_companions = []
        return added

    def _finish_known_companion(self, image_path, known):
        """Import sidecar and report the path while its owner stays locked.

        Keyword creation/tagging and the sidecar helper ordinarily commit
        internally. Hold those commits so they cannot release ownership
        between validation, tagging and the callback. The full/hash path
        uses the same guard as stat settling.
        """
        ownership = (
            known["folder_id"], known["owner_filename"], image_path.name,
        )
        with self.db._commits_held():
            if not self._lock_companion_owner(known["owner_id"], ownership):
                return False
            self._import_companion_sidecar(image_path, known)
            self._credit_known_companion(
                known["owner_id"], str(image_path),
                known["folder_id"], known["owner_filename"],
            )
        return True

    def _import_companion_sidecar(self, image_path, known):
        """Import the validated companion's additive sidecar keywords.

        Import shared-stem XMP too: the RAW can vanish after the JPEG is
        handled, even if it existed at discovery or at this call. Repeating
        the keyword import on the same owner is idempotent and preserves
        pending removals, just as the old insert-and-merge path did.
        """
        xmp_path = image_path.with_suffix(".xmp")
        if not xmp_path.exists():
            return
        try:
            _import_keywords_for_photo(
                self.db, known["owner_id"], str(xmp_path),
            )
        except Exception:
            log.exception(
                "Failed to import companion sidecar %s for owner %s",
                xmp_path, known["owner_id"],
            )
            return
        self.db.commit_with_retry()

    def _reuse_existing_row(self, existing, full_path_str, stat, xmp_path, xmp_mtime):
        """``True`` when the cataloged row still vouches for the file."""
        db = self.db
        file_mtime = stat.st_mtime
        # Copies/restores can preserve mtime even when the
        # original is replaced. Check size from the same stat
        # before reusing metadata, hashes, and derived caches.
        file_unchanged = (
            existing["file_mtime"] == file_mtime
            and existing["file_size"] == stat.st_size
        )
        xmp_unchanged = existing["xmp_mtime"] == xmp_mtime
        # Re-process if ExifTool never ran for this photo (both
        # timestamp and exif_data are NULL). Photos with genuinely
        # missing timestamps (screenshots, exports) will have
        # exif_data set after one extraction attempt.
        # Also flag rows where a RAW file has absurdly small
        # dimensions (<1000px) — that's the embedded JPEG thumb
        # leaking through when ExifTool's File group was missing
        # on the original scan.
        dims_suspect = (
            existing["extension"] in RAW_EXTENSIONS
            and existing["width"] is not None
            and existing["width"] < 1000
        )
        metadata_missing = (
            not existing["exif_extracted"]
            and (
                self.repair_missing_metadata
                or existing["timestamp"] is None
                or dims_suspect
            )
        ) or existing["summary_needs_extract"]
        existing_file_hash = existing["file_hash"]
        hash_needs_repair = (
            existing["file_size"] == 0
            and existing_file_hash == EMPTY_FILE_SHA256
        ) or (
            stat.st_size > 0 and existing_file_hash is None
        )

        if (
            file_unchanged and xmp_unchanged
            and not metadata_missing
            and not hash_needs_repair
        ):
            self._credit_reused_row(
                existing["id"], full_path_str,
                existing["folder_id"], existing["filename"],
            )
            return True

        # XMP changed: re-import keywords
        if not xmp_unchanged and xmp_mtime is not None:
            _import_keywords_for_photo(db, existing["id"], str(xmp_path))
            db.scanning.set_xmp_mtime(existing["id"], xmp_mtime)
            db.commit_with_retry()
        elif not xmp_unchanged:
            # Sidecar deleted: clear the stored mtime so the row
            # converges instead of looking "XMP changed" on every
            # later scan (this skip path never reaches the main
            # loop, so nothing else would ever reset it).
            db.scanning.set_xmp_mtime(existing["id"], None)
            db.commit_with_retry()

        if (
            file_unchanged
            and not metadata_missing
            and not hash_needs_repair
        ):
            self._credit_reused_row(
                existing["id"], full_path_str,
                existing["folder_id"], existing["filename"],
            )
            return True
        return False

    def _credit_reused_row(self, photo_id, full_path_str, folder_id, filename):
        self.processed_count += 1
        self.counts["indexed"] += 1
        self.indexed_photo_ids.add(photo_id)
        self._reported_identities[photo_id] = (folder_id, filename)
        if self.photo_callback:
            self.photo_callback(photo_id, full_path_str)
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)

    def _credit_known_companion(
        self, owner_id, full_path_str, owner_folder_id, owner_filename,
    ):
        """Dispose of a companion JPEG its RAW's row already holds.

        ``photo_callback`` gets the RAW's id: that row is where the file is
        cataloged (callers such as import-in-place count the path as
        indexed). It is not ``indexed`` again, the RAW was.
        """
        self.processed_count += 1
        self.counts["known_companions"] += 1
        self._reported_identities[owner_id] = (owner_folder_id, owner_filename)
        if self.photo_callback:
            self.photo_callback(owner_id, full_path_str)
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)

    # -- metadata ----------------------------------------------------

    def _extract_metadata(self):
        """Batch-read ExifTool metadata; returns the paths it was asked for."""
        # Batch extract metadata via ExifTool only for files that need processing
        paths_to_extract = [str(ip) for ip in self.files_to_process]
        if paths_to_extract and self.status_callback:
            metadata_total = len(paths_to_extract)
            self._emit_status(
                f"Extracting metadata (0 / {metadata_total} files)...",
                phase_current=0,
                phase_total=metadata_total,
                phase_label="Extracting metadata",
            )
        self._check_cancelled()

        self.metadata_map = (
            extract_metadata(
                paths_to_extract,
                progress_callback=self._metadata_progress,
                checkpoint=self._check_cancelled,
            )
            if paths_to_extract else {}
        )
        self._check_cancelled()
        return paths_to_extract

    def _metadata_progress(self, current, total):
        self._emit_status(
            f"Extracting metadata ({current} / {total} files)...",
            phase_current=current,
            phase_total=total,
            phase_label="Extracting metadata",
        )

    # -- hashing -----------------------------------------------------

    def _pause_pending(self):
        return self.pause_check is not None and self.pause_check()

    def _check_cancelled_no_park(self):
        """Cancel probe safe to call INSIDE ``_claim_worker_count``.

        Never parks. When ``cancel_only_check`` was supplied the
        pause/cancel probes are strictly non-parking, so a Pause
        arriving between the two calls raises ``_ScanPauseRequested``
        instead of triggering the caller's parking ``cancel_check``.
        That lets the enclosing frame drain workers and release CPU
        permits before parking on the outer ``_check_cancelled`` call.

        When ``cancel_only_check`` is not supplied this falls back to
        the pause-then-parking-cancel sequence with the race window
        Codex flagged — pause set between the two calls parks the job
        while it still holds the lease. Not worse than the pre-fix
        behavior, and every pipeline caller now wires the non-parking
        probe.
        """
        if self._pause_pending():
            raise _ScanPauseRequested()
        if self.cancel_only_check is not None:
            if self.cancel_only_check():
                raise ScanCancelled("scan cancelled")
            return
        self._check_cancelled()

    def _iter_features(self):
        """Yield ``(image_path, (phash, file_hash))`` in queue order.

        Computes phash + file_hash in parallel across all files that need
        processing. These are the two per-file operations that actually read
        every byte of the image; everything else in the loop is cheap DB or
        dict work. Results stream in order, so workers keep computing the tail
        while the main thread commits the head — no O(n) buffer of features.
        """
        if not self.files_to_process:
            return
        # Track remaining work outside the ``_claim_worker_count`` context so
        # a pause can drop the lease, park at the caller's pause-aware
        # ``cancel_check``, then reacquire a fresh lease for whatever is
        # left. Without this, ``_check_cancelled`` parking inside the pool
        # loop would hold every CPU permit for the entire pause — blocking
        # replacement scans, CPU inference, and model loads even though the
        # user asked this job to stop competing for resources.
        remaining = deque(
            zip(self.files_to_process, self.paths_to_extract, strict=True),
        )
        while remaining:
            # A standalone caller may provide a non-parking cancel probe.
            # Do not repeatedly claim permits and construct a process pool
            # while its independent pause probe remains true.
            if self._pause_pending():
                self._check_cancelled()
                if self._pause_pending():
                    time.sleep(0.05)
                continue
            with _claim_worker_count(
                [ip for ip, _ in remaining],
                cancel_check=(
                    self._check_cancelled
                    if self.cancel_check is not None else None
                ),
            ) as workers:
                if self.status_callback:
                    self._emit_status(
                        f"Hashing {len(remaining)} files "
                        f"({workers} worker{'s' if workers != 1 else ''})..."
                    )
                log.info(
                    "Scanner hashing granted %d worker(s) for %d files",
                    workers, len(remaining),
                )
                if workers > 1:
                    paused = yield from self._pool_features(workers, remaining)
                else:
                    paused = yield from self._inline_features(remaining)
            # Lease is released here (``_claim_worker_count`` exited).
            # Park until the caller resumes (or cancels) before rebuilding
            # the pool with the remaining files. ``_check_cancelled`` is
            # pause-aware: when the pipeline is pausing, ``cancel_check``
            # blocks inside it until resume, and raises ``ScanCancelled``
            # if the pause turns into a cancellation.
            if paused:
                self._check_cancelled()
        # The final result is consumed while this generator is suspended.
        # Check once more only after the last lease has unwound so a pause or
        # cancellation arriving during that consumer work cannot strand the
        # pool and its CPU permits.
        self._check_cancelled()

    def _pool_features(self, workers, remaining):
        """Hash on a process pool; returns ``True`` when a pause stopped it."""
        mp_ctx = multiprocessing.get_context(_SCAN_MP_METHOD)
        pool = ProcessPoolExecutor(max_workers=workers, mp_context=mp_ctx)
        paused = False
        try:
            # Bounded in-flight window instead of pool.map(): on Python
            # 3.11 Executor.map eagerly submits every input, so on a
            # 200k-file scan we would hold 200k queued futures in RAM.
            # A few submissions per worker is enough to keep them fed
            # while the main thread drains results in order.
            #
            # Pause is checked BEFORE ``_check_cancelled`` at every
            # boundary. The pipeline's ``cancel_check`` parks on a
            # pending pause, so calling it first would trap the
            # scanner inside the lease context; raising
            # ``_ScanPauseRequested`` while the pool is still
            # running lets the enclosing frame drain workers and
            # release CPU permits before we park.
            max_in_flight = workers * 4
            pending = deque()
            while remaining:
                self._check_cancelled_no_park()
                image_path, path_str = remaining[0]
                pending.append((
                    image_path, path_str,
                    pool.submit(_compute_file_features, path_str),
                ))
                remaining.popleft()
                if len(pending) >= max_in_flight:
                    done_path, _done_str, done_fut = pending[0]
                    self._check_cancelled_no_park()
                    result = self._feature_result(done_fut)
                    if self._pause_pending():
                        raise _ScanPauseRequested()
                    pending.popleft()
                    yield done_path, result
            while pending:
                done_path, _done_str, done_fut = pending[0]
                self._check_cancelled_no_park()
                result = self._feature_result(done_fut)
                if self._pause_pending():
                    raise _ScanPauseRequested()
                pending.popleft()
                yield done_path, result
        except _ScanPauseRequested:
            # Requeue every submitted-but-undrained item so the
            # next pass reruns them under a fresh lease. Their
            # workers are torn down below.
            paused = True
            for image_path, path_str, _fut in reversed(pending):
                remaining.appendleft((image_path, path_str))
            # Terminate now, wait below with the lease still
            # held so a replacement scan cannot claim the same
            # permits while old workers are still consuming CPU.
            for proc in list(getattr(pool, "_processes", {}).values()):
                # Best-effort kill of a worker that may already be
                # gone; shutdown(wait=True) below reaps the rest.
                with contextlib.suppress(Exception):
                    proc.terminate()
            pool.shutdown(wait=True, cancel_futures=True)
        except BaseException:
            # Actively terminate the worker processes so their CPU
            # work stops now, then wait for them to actually exit
            # before letting ``_claim_worker_count`` release its
            # CPU permits. ``shutdown(wait=False)`` alone would
            # unwind the lease while old workers were still
            # hashing — a replacement scan or CPU inference could
            # then receive the same permits and defeat the
            # process-wide budget, and ``JobRunner.shutdown()``
            # could report completion while hashing continued.
            for proc in list(getattr(pool, "_processes", {}).values()):
                # Must not replace the in-flight exception.
                with contextlib.suppress(Exception):
                    proc.terminate()
            pool.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            pool.shutdown(wait=True)
        return paused

    def _feature_result(self, fut):
        while True:
            # ``_check_cancelled_no_park`` raises
            # ``_ScanPauseRequested`` on pending pause
            # and only calls the parking probe if it
            # cannot be avoided (no ``cancel_only_check``
            # supplied). Closes the race Codex flagged
            # where ``_pause_pending`` returns False,
            # Pause fires, and the previously-called
            # ``_check_cancelled`` parked while still
            # inside the lease and pool.
            self._check_cancelled_no_park()
            try:
                return fut.result(timeout=0.2)
            except TimeoutError:
                continue

    def _inline_features(self, remaining):
        """Hash in this process; returns ``True`` when a pause stopped it."""
        paused = False
        while remaining:
            # ``_check_cancelled_no_park`` raises
            # ``_ScanPauseRequested`` on pending pause and
            # only invokes the parking probe if no
            # ``cancel_only_check`` was supplied. Caught and
            # translated to ``paused = True`` so this branch
            # keeps its "drop the lease, then park outside"
            # symmetry with the pool branch above.
            try:
                self._check_cancelled_no_park()
            except _ScanPauseRequested:
                paused = True
                break
            image_path, path_str = remaining[0]
            result = _compute_file_features(path_str)
            if self._pause_pending():
                paused = True
                break
            remaining.popleft()
            yield image_path, result
        return paused

    # -- main loop ---------------------------------------------------

    def _index_files(self):
        self._track_companion_groups()
        try:
            for image_path, (phash, file_hash) in self._iter_features():
                self._index_file(image_path, phash, file_hash)
                self._mark_group_member_seen(image_path)
            # Every group's last member has been seen by now; this only
            # matters if a path appeared in ``files_to_process`` twice.
            for key in list(self._waiting_companions):
                self._resolve_companion_group(key)
        except BaseException:
            # Waiting JPEGs have not been indexed or reported yet. Leave
            # them untouched if their group is interrupted: the partial
            # folder status makes the next scan retry the whole group.
            # Do not attempt new pairing writes after a database failure.
            # Per-file loop died mid-way (DB error, signal, etc). Roll back any
            # half-applied write so the partial-status UPDATE below runs on a
            # clean transaction, then flag every folder in scope as 'partial' so
            # callers can detect and re-scan.
            try:
                self.db.rollback()
            except Exception:
                log.exception("Rollback after scan failure also failed")
            try:
                self._update_folder_status("partial", only_from_partial=False)
            except Exception:
                log.exception("Failed to flag folders partial after scan failure")
            raise
        else:
            # Per-file loop completed cleanly. Clear any stale 'partial' flag on
            # scanned folders so a successful rescan restores full visibility.
            # Uses both the scan scope (root + restrict_dirs) AND the touched
            # folder ids: a successful no-op incremental scan has an empty
            # ``touched_folder_ids`` set but must still clear the badge for the
            # roots the user asked us to scan; a recursive scan that failed and
            # then succeeds needs the touched ids to reach touched subfolders.
            try:
                self._update_folder_status("ok", only_from_partial=True)
            except Exception:
                log.exception("Failed to clear partial flag after scan success")

    def _index_file(self, image_path, phash, file_hash):
        """Catalog one hashed file: row, metadata, caches, keywords."""
        db = self.db
        # File stats — first touch of the path in this loop. A file
        # deleted/renamed between discovery and here must skip, not
        # abort the scan and flag every folder in scope 'partial'.
        try:
            stat = image_path.stat()
        except OSError:
            log.info("File vanished during scan, skipping: %s", image_path)
            self._skip_file("vanished")
            return

        folder_id = self._ensure_folder(image_path.parent)
        self.touched_folder_ids.add(folder_id)
        file_size = stat.st_size
        file_mtime = stat.st_mtime
        if file_size == 0 and file_hash == EMPTY_FILE_SHA256:
            log.warning(
                "Empty image file detected; skipping duplicate identity hash: %s",
                image_path,
            )
            file_hash = None
        elif file_hash is None and file_size > 0:
            log.warning(
                "Could not read %s to hash it; it will be retried on the next scan",
                image_path,
            )

        # XMP sidecar
        xmp_path = image_path.with_suffix(".xmp")
        try:
            xmp_mtime = xmp_path.stat().st_mtime
        except OSError:
            xmp_mtime = None

        meta = self._read_file_metadata(image_path)

        # Pre-check: capture prior content identity AND whether the
        # row existed before add_photo touches it. Existing rows take
        # the content-change invalidation below; brand-new rows take
        # the cheaper recycled-rowid probe right after the insert, so
        # a large initial scan doesn't pay for O(N) UPDATE + commit
        # round-trips it has no reason to make.
        existing_row = db.scanning.content_baseline(folder_id, image_path.name)
        row_already_existed = existing_row is not None
        prev_file_hash = existing_row["file_hash"] if existing_row else None

        if not row_already_existed and self._keep_known_companion(
            image_path, folder_id, file_size, file_mtime, file_hash, meta,
        ):
            return

        # Process may refresh metadata for cataloged photos, but it must
        # never admit a filesystem path as a side effect. A row can
        # disappear between repair-scope resolution and this point; skip
        # that race rather than recreating it through add_photo().
        if not self.allow_photo_inserts and not row_already_existed:
            log.info(
                "Update-only scan skipped uncataloged file: %s", image_path,
            )
            self._skip_file("skipped_uncataloged")
            return

        item = _IndexedFile(
            image_path=image_path, folder_id=folder_id,
            file_size=file_size, file_mtime=file_mtime,
            file_hash=file_hash, phash=phash,
            xmp_path=xmp_path, xmp_mtime=xmp_mtime, meta=meta,
            row_already_existed=row_already_existed,
            prev_file_hash=prev_file_hash,
        )
        if (
            not row_already_existed
            and image_path.suffix.lower() in _COMPANION_JPEG_EXTENSIONS
        ):
            # Decide whether this JPEG is a photo or a RAW's companion
            # before it gets a row (see ``_resolve_companion_group``).
            self._waiting_companions[_companion_group_key(image_path)].append(
                item,
            )
            return
        self._catalog_file(item)

    def _catalog_file(self, item):
        """Insert or refresh ``item``'s row, then its keywords and callback."""
        db = self.db
        image_path = item.image_path
        xmp_path = item.xmp_path
        file_hash = item.file_hash
        photo_id = self._add_photo(item)
        self._write_photo_columns(photo_id, item)
        self._heal_changed_content(photo_id, item)

        # Import XMP keywords if sidecar exists — must land BEFORE the
        # duplicate auto-resolve hook below, so if this row turns out to be
        # the loser its keywords are visible to apply_duplicate_resolution's
        # metadata query and get merged onto the winner. Otherwise the
        # keywords would be stranded on the rejected row.
        if xmp_path.exists():
            _import_keywords_for_photo(db, photo_id, str(xmp_path))
        # Keywords Lightroom wrote into the file itself (JPEG, TIFF, DNG).
        # This path runs whenever the file is new or changed, which is also
        # when such a write lands: it rewrites the file, not a sidecar.
        _import_embedded_keywords_for_photo(db, photo_id, item.meta.file_meta)

        # Trigger duplicate auto-resolve now that file_hash AND XMP keywords
        # are committed. add_photo was called without the hash, so the hook
        # there was a no-op — we own firing it here.
        if file_hash is not None:
            db.check_and_resolve_duplicates_for_hash(file_hash)

        self._reported_identities[photo_id] = (
            item.folder_id, item.image_path.name,
        )
        if self.photo_callback:
            self.photo_callback(photo_id, str(image_path))

        self.processed_count += 1
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)

    # -- pairing a JPEG before it gets a row --------------------------
    #
    # A camera JPEG beside its RAW used to be inserted as its own photo and
    # merged into the RAW by the end-of-scan pairing pass. Between the two
    # its id was live: ``photo_callback`` reported it, so the pipeline's
    # and import-in-place's collections, thumbnail queue and counts kept a
    # deleted id, and because ``photos`` has no AUTOINCREMENT the next
    # insert could take that id and join those collections. The scan now
    # makes the pairing decision first and records a paired JPEG straight
    # onto its RAW's row, so the transient row never exists.

    def _track_companion_groups(self):
        """Count each same-stem group's RAW/JPEG files in this scan.

        A JPEG waits until the loop has seen every file of its group, so
        its RAW (which may sort after it: ``IMG.jpg`` < ``IMG.nef``) has a
        row with fresh metadata, and a group's JPEGs are decided together
        the way the end-of-scan pass would decide them.
        """
        self._unseen_group_members = defaultdict(int)
        self._waiting_companions = defaultdict(list)
        for image_path in self.files_to_process:
            ext = image_path.suffix.lower()
            if (
                ext in _COMPANION_RAW_EXTENSIONS
                or ext in _COMPANION_JPEG_EXTENSIONS
            ):
                self._unseen_group_members[
                    _companion_group_key(image_path)
                ] += 1

    def _mark_group_member_seen(self, image_path):
        key = _companion_group_key(image_path)
        remaining = self._unseen_group_members.get(key)
        if remaining is None:
            return
        remaining -= 1
        if remaining > 0:
            self._unseen_group_members[key] = remaining
            return
        del self._unseen_group_members[key]
        if key in self._waiting_companions:
            self._resolve_companion_group(key)

    def _resolve_companion_group(self, key):
        """Pair the group's uncataloged JPEGs, or catalog them as photos.

        The candidates are the cataloged rows of the group plus each waiting
        JPEG as a row-less member, decided by the same
        ``_pick_compatible_raw_jpeg_pairs`` the end-of-scan pass uses, so
        #1970's one-companion rule and the metadata checks hold unchanged.
        """
        items = self._waiting_companions.pop(key)
        folder_id = items[0].folder_id
        members = _pairing_rows_for_stem(self.db, folder_id, key[1])
        cataloged = {m["filename"] for m in members}
        members.extend(
            self._uncataloged_member(item) for item in items
            if item.image_path.name not in cataloged
        )
        members.sort(key=lambda m: m["filename"])
        owners = {
            jpeg["filename"]: raw
            for raw, jpeg in _pick_compatible_raw_jpeg_pairs(
                members, log_conflicts=False,
            )
            if jpeg["id"] is None and raw["id"] is not None
        }
        for item in items:
            owner = owners.get(item.image_path.name)
            if owner is not None and self._attach_companion(item, owner):
                continue
            # A photo of its own, or a RAW that changed under us: the
            # ordinary path (whose end-of-scan pass still pairs it, with
            # ``photo_merged_callback`` reporting the merge).
            self._catalog_file(item)

    def _uncataloged_member(self, item):
        """``item`` shaped like a ``_pairing_rows_for_stem`` row, ``id`` None."""
        file_meta = item.meta.file_meta
        summary = exif_summary_columns(file_meta) if file_meta else {}
        return {
            "id": None,
            "folder_id": item.folder_id,
            "filename": item.image_path.name,
            "extension": item.image_path.suffix.lower(),
            "timestamp": item.meta.timestamp,
            "camera_make": summary.get("camera_make"),
            "camera_model": summary.get("camera_model"),
            "companion_path": None,
        }

    def _attach_companion(self, item, owner):
        """Record ``item`` as ``owner``'s companion without a row; True if done.

        Does what inserting the JPEG and merging it used to leave on the
        RAW: ``companion_path``, the import identity re-import checks, the
        JPEG-only metadata filling the RAW's gaps, the JPEG's embedded and
        sidecar keywords, and dropping the RAW's companion-derived display
        caches. A changed companion takes the same path with fresh values.
        Returns False, writing nothing, when the owner row changed since it
        was read or the JPEG gained a row meanwhile.
        """
        db = self.db
        image_path = item.image_path
        name = image_path.name
        owner_id = owner["id"]
        ownership = (owner["folder_id"], owner["filename"], name)
        try:
            with db._commits_held():
                # The guarded write takes the writer lock before anything
                # else, so neither check below can go stale before commit.
                if not db.companion_pairing.claim_companion(
                    owner_id, owner["folder_id"], owner["filename"], name,
                ):
                    raise _CompanionAttachRefused
                if db.companion_pairing.photo_exists(item.folder_id, name):
                    raise _CompanionAttachRefused
                # Re-check compatibility under the writer lock. The identity
                # predicates above (id, folder, filename, companion_path)
                # miss a concurrent metadata UPDATE on the same row — a
                # same-stem replacement RAW from a different exposure or
                # camera that another scanner rewrote between
                # ``_resolve_companion_group`` reading its timestamp/camera
                # columns and this guarded UPDATE. Without this re-check the
                # pairing would commit on a stale compatibility decision and
                # permanently attach unrelated files.
                if not self._owner_still_compatible(owner_id, item):
                    raise _CompanionAttachRefused
                db.companion_pairing.record_identity(
                    owner_id, name, item.file_size, item.meta.timestamp,
                    item.file_hash, item.file_mtime,
                )
                self._apply_companion_gap_fill(
                    owner_id, self._companion_columns(item.meta), ownership,
                )
                _import_embedded_keywords_for_photo(
                    db, owner_id, item.meta.file_meta,
                )
                self._import_companion_sidecar(
                    image_path, {"owner_id": owner_id},
                )
                self.processed_count += 1
                self.counts["merged_companions"] += 1
        except _CompanionAttachRefused:
            # ``_commits_held`` rolled back already, unless the connection
            # cannot hold commits; the refusals precede every other write.
            db.rollback()
            return False
        self._publish_attached_companion(image_path, owner, ownership)
        if self.progress_callback:
            self.progress_callback(self.processed_count, self.total)
        return True

    def _owner_still_compatible(self, owner_id, item):
        """Whether the owner row, as it stands now, still pairs with ``item``.

        Only a row that is present and incompatible refuses the attach; the
        guarded UPDATE ahead of this already required the row under the
        writer lock.
        """
        current = self.db.companion_pairing.capture_columns(owner_id)
        if current is None:
            return True
        jpeg_summary = (
            exif_summary_columns(item.meta.file_meta)
            if item.meta.file_meta else {}
        )
        return _companions_compatible(
            {
                "timestamp": current["timestamp"],
                "camera_make": current["camera_make"],
                "camera_model": current["camera_model"],
            },
            {
                "timestamp": item.meta.timestamp,
                "camera_make": jpeg_summary.get("camera_make"),
                "camera_model": jpeg_summary.get("camera_model"),
            },
        )

    def _publish_attached_companion(self, image_path, owner, ownership):
        """Drop the RAW's display caches and report the committed attach."""
        owner_id = owner["id"]
        # Report AFTER ``_commits_held`` releases: the thumbnail worker runs
        # on a separate connection and only sees committed data. If the
        # callback fired inside the guard, a queue entry whose canonical
        # path falls back to the companion (missing RAW) would be checked
        # against a row where ``companion_path`` is still NULL and
        # ``_still_owns`` would drop it. Unlike ``_finish_known_companion``,
        # which handles an unchanged companion whose ``companion_path`` was
        # committed by an earlier scan, this attach is the write that first
        # makes the pairing visible.
        #
        # Invalidate the RAW's display cache before the callback: a
        # callback that raises leaves the pairing committed, and the next
        # incremental scan takes ``_finish_known_companion`` instead,
        # which does not re-invalidate. Running the post-commit filesystem
        # actions first means a stale pre-pairing render cannot survive a
        # callback failure, and ``_run_post_commit_fs_actions`` already
        # catches per-action errors so it will not skip the callback.
        if self.vireo_dir:
            actions = []
            _defer_primary_display_cache_invalidation(
                actions, {"id": owner_id}, self.vireo_dir, self.thumb_cache_dir,
            )
            _run_post_commit_fs_actions(actions)
        if self.photo_callback:
            # Revalidate ownership before publishing ``owner_id``. The
            # attach commit was released by the caller so the post-commit FS
            # actions could see it; in that window another connection can
            # delete the RAW and SQLite can reuse its rowid for an
            # unrelated photo. Without this check, an incremental scan
            # that processes only a new JPEG beside an unchanged
            # cataloged RAW would publish the stale id to the pipeline
            # or import collection — and no end-of-scan merge callback
            # would correct it because the JPEG never had a row of its
            # own. Taking the writer lock under ``_commits_held`` makes
            # the check-then-publish atomic against concurrent deletes.
            prepare = getattr(self.photo_callback, "prepare_publication", None)
            publication = (
                prepare(owner_id, str(image_path)) if prepare is not None
                else contextlib.nullcontext(
                    lambda: self.photo_callback(owner_id, str(image_path)),
                )
            )
            # A streaming receiver waits for Pause and queue capacity before
            # the writer lock, then publishes through a nonblocking callback.
            # Ownership is revalidated AFTER that wait, keeping rowid reuse
            # protection without preventing the consumer's thumbnail commits.
            with publication as publish, self.db._commits_held():
                owner_still_valid = self._lock_companion_owner(
                    owner_id, ownership,
                )
                if owner_still_valid:
                    self._reported_identities[owner_id] = (
                        owner["folder_id"], owner["filename"],
                    )
                    publish()
                else:
                    log.warning(
                        "Companion attach for %s lost RAW owner %s before "
                        "publishing; the next scan will catalog the JPEG",
                        image_path, owner_id,
                    )
        else:
            self._reported_identities[owner_id] = (
                owner["folder_id"], owner["filename"],
            )

    def _keep_known_companion(
        self, image_path, folder_id, file_size, file_mtime, file_hash, meta,
    ):
        """Leave an unchanged companion JPEG in its RAW's row; True if it was.

        A companion whose bytes changed (or that has no stored identity to
        compare) returns False and is re-paired with its RAW without a row
        of its own (``_resolve_companion_group`` -> ``_attach_companion``),
        which refreshes the stored identity, fills the RAW's gaps from it,
        carries its new keywords over, and drops the RAW's
        companion-derived caches.
        """
        known = self.known_companions.lookup(folder_id, image_path.name)
        if known is None:
            return False
        ownership = (folder_id, known["owner_filename"], image_path.name)
        with self.db._commits_held():
            if not self._lock_companion_owner(known["owner_id"], ownership):
                return False
            return self._keep_locked_companion(
                image_path, known, file_size, file_mtime, file_hash, meta,
            )

    def _keep_locked_companion(
        self, image_path, known, file_size, file_mtime, file_hash, meta,
    ):
        """Keep metadata, identity, sidecar and credit under the owner's lock."""
        if file_hash is None and file_size > 0:
            # Unreadable right now. Re-merging would replace a verified
            # identity with an unhashed one; keep the catalog's and let the
            # next scan, which sees the stored mtime is stale, try again.
            log.info(
                "Could not read companion %s to check it; keeping its RAW's "
                "stored identity", image_path,
            )
        elif _companion_bytes_unchanged(known, file_size, file_hash):
            if meta.file_meta:
                # A previous extraction may have omitted JPEG keywords. The
                # fresh metadata is authoritative even when bytes match;
                # reuse durable offered-key suppression for user removals.
                _import_embedded_keywords_for_photo(
                    self.db, known["owner_id"], meta.file_meta,
                )
                companion_columns = self._companion_columns(meta)
                # Apply the gap fill inline in this same transaction as
                # the file_mtime update: a successful commit means both
                # are durable, a rollback means neither. Handles the
                # common case where the owner RAW was already re-read
                # earlier this scan (its JPEG-only columns are already
                # cleared, so this UPDATE fills them). When the companion
                # comes first the fill is a no-op here (columns not
                # cleared yet) and ``_write_photo_columns`` will fold
                # the same fill into the owner's clear-to-NULL UPDATE
                # via ``_pending_companion_fills`` below.
                self._apply_companion_gap_fill(
                    known["owner_id"], companion_columns,
                    (known["folder_id"], known["owner_filename"], image_path.name),
                )
                self._pending_companion_fills[known["owner_id"]] = (
                    (known["folder_id"], known["owner_filename"], image_path.name),
                    companion_columns,
                )
            # Only record the fresh stat when the fill actually ran, or
            # when the owner is unchanged this scan and no retry marker
            # is already pending (so nothing was cleared that would need
            # a refill). If ExifTool transiently omits this companion
            # from the metadata result and the owner was re-read earlier
            # this scan, ``_write_photo_columns`` has nulled this
            # identity's file_mtime so a retry fires on the next
            # incremental scan; writing the current mtime here would
            # stat-skip the companion forever and the JPEG-only columns
            # (e.g. ``lens``) would stay cleared until a full rescan.
            # ``reprocessed_row_ids`` is populated only by the incremental
            # pre-pass, so on a full rescan or on a later incremental
            # scan where the owner's row is unchanged, the companion's
            # already-NULL ``file_mtime`` is itself the signal that a
            # retry is pending: respect it until metadata is available.
            if meta.file_meta or (
                known["file_mtime"] is not None
                and known["owner_id"] not in self.reprocessed_row_ids
            ):
                self.db.companion_pairing.set_identity_mtime(
                    known["owner_id"], image_path.name, file_mtime,
                )
            self.db.commit_with_retry()
        else:
            return False
        return self._finish_known_companion(image_path, known)

    def _companion_columns(self, meta):
        """The columns pairing reads from a companion row, from fresh metadata.

        Shaped like ``_read_metadata_transfer_rows``'s companion row, so
        ``_fill_primary_metadata_gaps`` treats it the way the merge would
        have treated the same file re-inserted as a row.
        """
        file_meta = meta.file_meta
        columns = {
            "timestamp": meta.timestamp,
            "rating": 0,
            "flag": "none",
            "latitude": meta.latitude,
            "longitude": meta.longitude,
            "exif_data": (
                json.dumps(file_meta) if self.extract_full_metadata else "{}"
            ),
            "width": meta.width,
            "height": meta.height,
        }
        summary = exif_summary_columns(file_meta)
        for column in EXIF_SUMMARY_COLUMNS:
            columns[column] = summary.get(column)
        return columns

    def _lock_companion_owner(self, owner_id, ownership):
        """Revalidate ownership while acquiring the transaction's writer lock.

        Photo IDs can be recycled between commits. The guarded no-op write
        prevents a concurrent deletion/replacement between this check and
        the metadata write; the caller's commit releases the lock.
        """
        folder_id, filename, companion = ownership
        return self.db.companion_pairing.lock_owner(
            owner_id, folder_id, filename, companion,
        )

    def _apply_companion_gap_fill(self, owner_id, companion_columns, ownership):
        """Fill the owner's gaps from the companion in the current transaction.

        Reads the owner's columns as they stand right now (so a prior
        clear-to-NULL in this transaction is visible) and runs
        ``_fill_primary_metadata_gaps`` without committing. The caller
        owns the commit, so the fill lands atomically with whatever
        other work the caller is already staging — see
        ``_keep_known_companion`` and ``_write_photo_columns``.
        """
        if not self._lock_companion_owner(owner_id, ownership):
            return
        owner_columns, _ = _read_metadata_transfer_rows(
            self.db, {"id": owner_id}, {"id": owner_id},
        )
        if owner_columns is None:
            return
        _fill_primary_metadata_gaps(
            self.db, {"id": owner_id}, owner_columns, companion_columns,
        )

    def _refill_owners_from_companions(self):
        """Safety-net fill for companions whose owner wasn't re-read.

        The primary paths — inline fill in ``_keep_known_companion`` when
        the owner is already written, and the fill folded into
        ``_write_photo_columns``'s UPDATE when the companion ran first —
        both keep the fill transactional with the clear-to-NULL commit.
        An entry still here is one neither path consumed (e.g. the full-
        scan case where the owner is unchanged and never re-read, so no
        clear happened); the fill is idempotent (only writes NULL
        columns) so a replay here just no-ops.
        """
        if not self._pending_companion_fills:
            return
        for owner_id, (ownership, columns) in self._pending_companion_fills.items():
            self._apply_companion_gap_fill(owner_id, columns, ownership)
        self.db.commit_with_retry()
        self._pending_companion_fills = {}

    def _read_file_metadata(self, image_path):
        # Get pre-extracted metadata for this file
        file_meta = self.metadata_map.get(str(image_path), {})
        file_group = file_meta.get("File", {})
        exif_group = file_meta.get("EXIF", {})
        composite = file_meta.get("Composite", {})

        # Dimensions from ExifTool (works for all file types including RAW)
        width, height = _extract_dimensions(exif_group, file_group, extension=image_path.suffix.lower())

        # Fallback if ExifTool didn't provide dimensions
        if width is None or height is None:
            ext = image_path.suffix.lower()
            if ext in RAW_EXTENSIONS:
                try:
                    import rawpy

                    with rawpy.imread(str(image_path)) as raw:
                        width = raw.sizes.width
                        height = raw.sizes.height
                except Exception:
                    log.debug("Could not read RAW dimensions from %s", image_path)
            else:
                try:
                    with Image.open(str(image_path)) as img:
                        width, height = img.size
                except Exception:
                    log.debug("Could not read dimensions from %s", image_path)

        # Timestamp from ExifTool
        timestamp = _extract_timestamp(exif_group)

        # Focal length is written via the EXIF summary columns loop
        # below (see ``EXIF_SUMMARY_COLUMNS``) so a rescan that loses
        # the tag clears the column instead of leaving a stale value.

        # Burst ID (ImageUniqueID)
        burst_id = exif_group.get("ImageUniqueID")
        if burst_id:
            burst_id = str(burst_id)

        # GPS coordinates — ExifTool with -n gives decimal degrees directly
        latitude = composite.get("GPSLatitude")
        if latitude is None:
            latitude = exif_group.get("GPSLatitude")
        longitude = composite.get("GPSLongitude")
        if longitude is None:
            longitude = exif_group.get("GPSLongitude")

        return _FileMetadata(
            file_meta=file_meta, width=width, height=height,
            timestamp=timestamp, burst_id=burst_id,
            latitude=latitude, longitude=longitude,
        )

    def _add_photo(self, item):
        """Insert (or find) the row and credit it; returns the photo id."""
        vireo_dir = self.vireo_dir
        # Keep insertion and inherited-membership cleanup under the same
        # writer lock. A second connection may legitimately add the new
        # photo to a collection as soon as the row becomes visible; cleanup
        # after add_photo commits would mistake that membership for stale.
        with self.db._commits_held():
            photo_id, inserted = self.db.add_photo(
                folder_id=item.folder_id,
                filename=item.image_path.name,
                extension=item.image_path.suffix.lower(),
                file_size=item.file_size,
                file_mtime=item.file_mtime,
                xmp_mtime=item.xmp_mtime,
                timestamp=item.meta.timestamp,
                width=item.meta.width,
                height=item.meta.height,
                return_inserted=True,
            )
            # The pre-check can miss a concurrent winning INSERT. Only
            # the actual inserter may remove inherited memberships.
            if inserted:
                self._drop_inherited_collection_membership(photo_id)
        # Credit the row after the insert and cleanup are durable. Later
        # cache invalidation or callbacks may fail without losing this count.
        self.counts["indexed"] += 1
        self.indexed_photo_ids.add(photo_id)

        # A brand-new row may have claimed a *recycled* rowid (see
        # ``purge_cached_files_for_recycled_id``). Cached derivatives
        # from the id's previous owner would otherwise be served as
        # this photo's — a wrong-bird Life List card. The index makes
        # this an O(1) set lookup per insert; only ids that actually
        # collide do real work.
        if (
            inserted
            and vireo_dir
            and purge_cached_files_for_recycled_id(
                self.thumb_cache_dir or os.path.join(vireo_dir, "thumbnails"),
                photo_id,
                id_index=self.recycled_id_index,
                vireo_dir=vireo_dir,
                db=self.db,
                file_mtime=item.file_mtime,
            )
        ):
            self.invalidated_photo_ids.add(photo_id)
        return photo_id

    def _init_collection_membership_cache(self):
        """Track collection edits on this connection, including rolled-back ones.

        data_version detects other connections' commits only. TEMP triggers
        notify a monotonic Python counter for this connection's collection
        writes; unlike a SQL counter, rollback cannot reuse a cache version.
        The triggers/function last only as long as the connection and never
        change the persistent catalog schema.
        """
        self.db.scanning.install_collection_change_counter()
        self._collection_named_ids = None
        self._collection_named_ids_version = None

    def _drop_inherited_collection_membership(self, photo_id):
        """Take a just-inserted photo out of collections it never joined.

        A static collection can still list the id of a photo that no longer
        exists (a companion JPEG row an older build inserted and merged
        away after the pipeline or import collection captured it, or a
        deletion from before deletes cleaned collections). SQLite hands the
        highest freed id to the next insert, which would then appear in
        that collection. A photo that did not exist until now cannot be a
        member of anything, so any listing of its id is stale. Reuse the
        named-id set until either another connection commits (data_version)
        or this connection edits collections (the non-transactional counter).
        Check both after the INSERT has acquired its writer lock, so a stale
        UI save between insert commits cannot become a new photo's membership.
        """
        version = (
            self.db.scanning.data_version(),
            self.db.scanning.collection_revision(),
        )
        if (
            self._collection_named_ids is None
            or version != self._collection_named_ids_version
        ):
            self._collection_named_ids = (
                self.db.photo_ids_named_by_collections()
            )
            self._collection_named_ids_version = version
        if photo_id not in self._collection_named_ids:
            return
        self._collection_named_ids.discard(photo_id)
        rewritten = self.db.remap_collection_photo_ids({photo_id: None})
        # This remap only removed photo_id, already discarded above. Keep
        # the cache current without traversing all other memberships again.
        self._collection_named_ids_version = (
            version[0], self.db.scanning.collection_revision(),
        )
        self.db.commit_with_retry()
        log.warning(
            "New photo %d reused the id of a deleted photo that %d "
            "collection(s) still listed; removed the stale entry so the "
            "new photo does not join them",
            photo_id, rewritten,
        )

    def _write_photo_columns(self, photo_id, item):
        # Update metadata columns (also fixes existing photos that were
        # inserted before ExifTool metadata was available)
        meta = item.meta
        file_meta = meta.file_meta
        file_hash = item.file_hash
        file_size = item.file_size
        row_already_existed = item.row_already_existed
        updates, update_params = self._identity_column_updates(item)
        pending_companion_fill = self._take_pending_companion_fill(
            photo_id, row_already_existed,
        )
        if file_meta:
            # Promoted EXIF summary columns (universal filter fields).
            # Written whenever ExifTool ran, independent of whether the
            # full JSON blob is stored below. Absent columns are cleared
            # to NULL rather than skipped so a rescan of a file whose
            # metadata lost a field (e.g. sidecar edited, replaced with a
            # different camera's file) doesn't leave stale values that
            # /api/photos/query and /api/filters/values keep matching.
            # When a companion gap fill is pending for this owner (its
            # JPEG companion was processed earlier this scan and found
            # unchanged), fold its JPEG-only values into the same UPDATE:
            # keep the RAW's value where it has one, else fall back to
            # the companion's. Keeps the clear-to-NULL atomic with the
            # fill — otherwise an interruption between a standalone
            # clear-commit and the end-of-scan refill would leave
            # columns like ``lens`` cleared with no retry trigger on the
            # next incremental scan.
            cols = exif_summary_columns(file_meta)
            for column in EXIF_SUMMARY_COLUMNS:
                value = cols.get(column)
                if value is None and pending_companion_fill is not None:
                    value = pending_companion_fill.get(column)
                updates.append(f"{column}=?")
                update_params.append(value)
        if file_meta and self.extract_full_metadata:
            updates.append("exif_data=?")
            update_params.append(json.dumps(file_meta))
        elif file_meta:
            # Store minimal marker so we know ExifTool ran (even when
            # extract_full_metadata is off) — prevents perpetual retry
            updates.append("exif_data=COALESCE(exif_data, ?)")
            update_params.append("{}")
        if row_already_existed:
            # add_photo is INSERT OR IGNORE, so the fresh stat values it
            # was passed never reach an existing row. Without these the
            # incremental pre-pass keeps comparing against the stale
            # stored mtime and re-processes a changed file on every scan
            # forever. Only advance file_mtime/file_size when the content
            # hash succeeded: _compute_file_features hashes every
            # processed file, so file_hash is None only when the bytes
            # couldn't be read (transient permission/I-O error). Marking
            # such a file's mtime current would make the next incremental
            # scan skip it forever with a stale hash and stale derived
            # caches; leaving the old mtime in place retries it instead.
            if file_hash is not None or file_size == 0:
                updates.extend(["file_mtime=?", "file_size=?"])
                update_params.extend([item.file_mtime, file_size])
            # xmp_mtime stays unconditional — the sidecar is a separate
            # file whose keyword import below runs regardless of image
            # hash success, and it may be None here: writing NULL is
            # correct (a deleted sidecar otherwise re-trips the "XMP
            # changed" check on every scan).
            updates.append("xmp_mtime=?")
            update_params.append(item.xmp_mtime)
        if updates:
            self.db.scanning.update_photo(photo_id, updates, update_params)
            if (
                row_already_existed
                and file_meta
                and pending_companion_fill is None
            ):
                # The clear-to-NULL above may have erased JPEG-only
                # columns (lens, etc.) that a prior pairing-merge filled
                # from this owner's companion. If the companion is read
                # later this scan it will refill them through
                # ``_pending_companion_fills``; if the scan is
                # interrupted first, null the companion's stored
                # ``file_mtime`` so the next incremental scan sees a
                # stale stat and re-reads the companion, replaying the
                # fill. Harmless no-op when the row has no
                # ``companion_identities`` entry (UPDATE touches 0 rows).
                self.db.companion_pairing.clear_identity_mtime(photo_id)
            self.db.commit_with_retry()

    def _identity_column_updates(self, item):
        """SET clauses for the file's metadata and content-hash columns."""
        meta = item.meta
        file_hash = item.file_hash
        file_size = item.file_size
        row_already_existed = item.row_already_existed
        prev_file_hash = item.prev_file_hash
        updates = []
        update_params = []
        if meta.timestamp is not None:
            updates.append("timestamp=?")
            update_params.append(meta.timestamp)
        if meta.width is not None:
            updates.append("width=?")
            update_params.append(meta.width)
        if meta.height is not None:
            updates.append("height=?")
            update_params.append(meta.height)
        if meta.latitude is not None:
            updates.extend(["latitude=?", "longitude=?"])
            update_params.extend([meta.latitude, meta.longitude])
        if item.phash is not None:
            updates.append("phash=?")
            update_params.append(item.phash)
        if meta.burst_id is not None:
            updates.append("burst_id=?")
            update_params.append(meta.burst_id)
        if file_hash is not None:
            updates.append("file_hash=?")
            update_params.append(file_hash)
            if row_already_existed and prev_file_hash != file_hash:
                # The stored baseline is being replaced, so any prior
                # integrity verdict applied to bytes that no longer
                # exist. Clear the verification markers rather than
                # carry them forward — the audit summary must only
                # claim "checked" for baselines verify_hashes (or an
                # explicit user accept) actually vouched for. A rescan
                # that recomputes the same hash leaves coverage intact.
                updates.append("hash_checked_at=NULL")
                updates.append("hash_status=NULL")
        elif file_size == 0:
            updates.append("file_hash=NULL")
            if row_already_existed and prev_file_hash is not None:
                updates.append("hash_checked_at=NULL")
                updates.append("hash_status=NULL")
            # Historical rows where the empty SHA leaked in are repaired
            # here by clearing file_hash above. We deliberately leave the
            # ``flag`` column untouched: a 'rejected' value could come
            # from the user (Browse / culling) just as easily as from
            # past duplicate auto-resolution, and we have no marker that
            # distinguishes them. Silently un-rejecting a user's
            # placeholder would be worse than leaving it as-is; the
            # duplicates page already flags empty-byte groups for
            # manual review.
        return updates, update_params

    def _take_pending_companion_fill(self, photo_id, row_already_existed):
        """The companion fill queued for this owner, if it still owns it."""
        pending_companion = (
            self._pending_companion_fills.pop(photo_id, None)
            if row_already_existed else None
        )
        if pending_companion is None:
            return None
        ownership, columns = pending_companion
        if self._lock_companion_owner(photo_id, ownership):
            return columns
        return None

    def _heal_changed_content(self, photo_id, item):
        """Content-change self-heal.

        When the computed hash differs from what was stored before this
        scan, derived caches are stale. Includes the NULL → concrete
        transition for legacy rows that predate hash tracking — we can't
        prove their caches match current bytes, so safer to flush and
        regenerate. Also fires when a file is truncated to zero bytes:
        file_hash is None in that branch (empty files don't carry
        duplicate identity), so the plain ``file_hash is not None`` guard
        would otherwise leave thumbnails and working copies from the old
        bytes in place. The zero-byte clause covers BOTH non-empty → empty
        and legacy NULL → empty: a pre-hash row whose file is now
        truncated still has thumbnails rendered from its old non-empty
        bytes, and before this fix the previous code path (which stored
        the concrete empty SHA) would have invalidated it via the NULL →
        concrete branch. Skips the empty → empty repair case (prev was
        already the empty SHA) because the bytes didn't actually change.
        Gated on ``row_already_existed`` so brand-new inserts
        (prev_file_hash is always NULL there) don't trigger pointless
        UPDATE + commit round-trips on large initial scans. Requires
        explicit vireo_dir; callers must pass it (scan can't guess
        because --db and --thumb-dir are independently configurable).
        """
        file_hash = item.file_hash
        prev_file_hash = item.prev_file_hash
        content_identity_changed = (
            file_hash is not None and prev_file_hash != file_hash
        ) or (
            item.file_size == 0
            and prev_file_hash != EMPTY_FILE_SHA256
        )
        if (item.row_already_existed
                and content_identity_changed
                and self.vireo_dir):
            _invalidate_derived_caches(
                self.db, self.vireo_dir, photo_id,
                thumb_cache_dir=self.thumb_cache_dir,
            )
            self.invalidated_photo_ids.add(photo_id)
            self.db.commit_with_retry()

    # -- post-processing ---------------------------------------------

    def _finish_catalog(self):
        db = self.db
        vireo_dir = self.vireo_dir
        counts = self.counts
        # Pair raw+JPEG companions: raw is primary, JPEG becomes companion_path.
        # Wrap post-processing so folder counts are always updated, even on failure.
        # On exception, roll back any uncommitted partial writes before updating
        # counts — otherwise update_folder_counts()'s commit would persist
        # half-applied pairing or working-copy records.
        try:
            # A JPEG merged into its RAW's row stops being its own photo, so
            # discount it: both files were counted on the way in but only the
            # RAW survives. Applied to the sink immediately (not just the
            # returned dict) so a caller reading counts after a later failure
            # in this block still sees the corrected number.
            self._refill_owners_from_companions()
            _merged_ids = _pair_raw_jpeg_companions(
                db, vireo_dir=vireo_dir, thumb_cache_dir=self.thumb_cache_dir,
            )
            # Discount only companions THIS scan counted. The pairing pass
            # queries the whole photos table, so it also cleans up pairs left
            # pending elsewhere in the catalog (an interrupted earlier scan,
            # an older build). Debiting a scoped scan for those would make it
            # undercount, and scanning a root with no new files while one
            # stale pair was pending would report -1 photos indexed.
            _mine = _merged_ids.keys() & self.indexed_photo_ids
            counts["merged_companions"] += len(_mine)
            counts["indexed"] -= len(_mine)
            self._report_merged_photos(_merged_ids)

            # Extract working copies for RAW photos (after pairing so companion is known).
            # Scope to the folders the caller just scanned so a fresh import doesn't
            # trigger library-wide backfill for every pre-existing large JPEG.
            # Match-mode mirrors what scan() actually traversed: restrict_dirs and
            # non-recursive scans only touch direct children, so the scope uses an
            # exact-folder match; a recursive walk from `root` matches the subtree.
            #
            # Deliberately do NOT forward ``progress_callback`` here: callers
            # like app.py's import job feed the scan callback into a shared
            # ``job["progress"]`` slot that gates downstream phase totals
            # (``scan_count = job["progress"]["total"]``). Emitting working-copy
            # (current, total) through the same callback would overwrite the
            # scan total with the working-copy total and visually jump the bar
            # backward. ``status_callback`` still announces the phase.
            if vireo_dir and not self.skip_working_copies:
                _extract_working_copies(
                    db, vireo_dir,
                    progress_callback=None,
                    status_callback=self.status_callback,
                    scope=self._working_copy_scope(),
                    cancel_check=self.cancel_check,
                )

            # Batched untracked-preview sweep. One os.listdir(previews/) for
            # the whole scan instead of one per invalidated photo — essential
            # when a rescan touches thousands of content-changed files.
            if self.invalidated_photo_ids:
                _sweep_untracked_previews_for_photos(
                    db, vireo_dir, self.invalidated_photo_ids,
                )
        except BaseException:
            db.rollback()
            raise
        finally:
            db.update_folder_counts()

    def _report_merged_photos(self, merged):
        """Tell ``photo_merged_callback`` which RAW absorbed each merged id."""
        if not merged or self.photo_merged_callback is None:
            return
        # Pairing has committed, so readers on other connections can see
        # the merge. Reacquire the writer lock before validating the RAW
        # and publishing its id; deletion and rowid reuse must not interleave
        # with a callback that updates an import's in-memory membership.
        companion_identities = getattr(merged, "companions", {})
        reported_identities = getattr(self, "_reported_identities", {})
        with self.db._commits_held():
            self.db.begin_immediate()
            for old_id, new_id in merged.items():
                row = self.db.companion_pairing.owner_row(new_id)
                expected = getattr(merged, "owners", {}).get(new_id)
                # The merged-away JPEG must still be the same file the
                # scanner reported under ``old_id``. If a concurrent delete
                # freed the id and SQLite reused it for an unrelated row
                # that pairing then merged, substituting ``new_id`` for
                # ``old_id`` in the receiver's collection would adopt an
                # unrelated RAW. A missing reported identity means none of
                # this receiver's scanner calls reported the id; pairing
                # also covers catalog rows outside those calls.
                reported_identity = reported_identities.get(old_id)
                pairing_identity = companion_identities.get(old_id)
                identity_matches = (
                    reported_identity is None
                    or pairing_identity is None
                    or reported_identity == pairing_identity
                )
                if (
                    row is None
                    or (row[0], row[1]) != expected
                    or not identity_matches
                ):
                    self.photo_merged_callback(old_id, None, None)
                else:
                    self.photo_merged_callback(
                        old_id, new_id, os.path.join(row[2], row[1]),
                    )
                    if reported_identity is not None:
                        reported_identities[new_id] = expected
                reported_identities.pop(old_id, None)

    def _working_copy_scope(self):
        if self.restrict_dirs is not None:
            # Use the bundle-filtered list — see ``effective_restrict_dirs``
            # above. Reusing the raw ``restrict_dirs`` here would let a
            # stale DB row inside an excluded bundle re-enter the
            # working-copy extractor, which reads ``folder_path/filename``
            # and re-trips the macOS TCC prompt the scan loop's guard
            # already skipped.
            return [(str(d), "exact") for d in self.effective_restrict_dirs]
        if not self.recursive:
            return [(str(self.root_path), "exact")]
        return [str(self.root_path)]

    def _log_summary(self):
        counts = self.counts
        processed_count = self.processed_count
        # Every file the loop disposed of landed in exactly one bucket. If this
        # ever trips, a new disposition was added without classifying it, and
        # the summary below would silently over-claim by that many photos —
        # log it rather than raising, since the scan's real work is committed
        # and a miscounted summary is no reason to fail the run.
        vanished_count = counts["vanished"]
        skipped_uncataloged_count = counts["skipped_uncataloged"]
        merged_count = counts["merged_companions"]
        known_companion_count = counts["known_companions"]
        indexed_count = counts["indexed"]
        accounted = (
            indexed_count + vanished_count + skipped_uncataloged_count
            + merged_count + known_companion_count
        )
        if accounted != processed_count:
            log.error(
                "Scan count invariant broken: indexed=%d + vanished=%d + "
                "skipped=%d + merged=%d + known companions=%d != processed=%d "
                "(a file disposition is unclassified)",
                indexed_count, vanished_count, skipped_uncataloged_count,
                merged_count, known_companion_count, processed_count,
            )

        # Report what reached the catalog, not what the walk turned up. These
        # used to be the same number ("Scan complete: %d photos indexed" logged
        # ``total``), which reads as a success line and stays reassuring
        # precisely when the scan achieved nothing — an archive share that
        # dropped mid-scan logged "984 photos indexed" having indexed zero.
        summary = f"Scan complete: {indexed_count} photos indexed"
        if merged_count:
            summary += f", {merged_count} JPEG(s) merged into their RAW"
        if known_companion_count:
            summary += (
                f", {known_companion_count} JPEG(s) already paired with "
                "their RAW"
            )
        if vanished_count:
            summary += f", {vanished_count} vanished"
        if skipped_uncataloged_count:
            summary += f", {skipped_uncataloged_count} uncataloged (skipped)"
        if (merged_count or known_companion_count or vanished_count
                or skipped_uncataloged_count):
            summary += f" of {self.total} discovered"
        log.info(summary)
