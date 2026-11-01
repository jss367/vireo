"""``scan()`` and ``_ScanRun``, the orchestrator that runs one scan's phases in order.

Each phase is a collaborator in its own module: ``discovery``
(``_Discovery``), ``folders`` (``_ScanFolders``), ``triage``
(``_IncrementalTriage``), ``features`` (``_FeatureHasher``), ``indexing``
(``_FileIndexer``), ``companion_keep`` and ``companion_attach``
(companion JPEGs), sharing the signals and tally in ``run_state``.
Metadata extraction and the end-of-scan pairing and working-copy pass
run here. The SQL lives in ``repositories/scanning.py`` (``db.scanning``).
"""

from __future__ import annotations

import errno
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from image_loader import is_excluded_scan_path
from metadata import extract_metadata
from scanner.cache_invalidation import _sweep_untracked_previews_for_photos
from scanner.companion_attach import _CompanionGroups
from scanner.companion_keep import _CompanionFills, _KnownCompanionKeeper
from scanner.companions import _KnownCompanions, _pair_raw_jpeg_companions
from scanner.discovery import _Discovery
from scanner.features import _FeatureHasher
from scanner.folders import _ScanFolders
from scanner.indexing import _CollectionMembership, _FileIndexer
from scanner.options import ScanControl, ScanHooks, ScanOptions
from scanner.run_state import _ScanSignals, _ScanTally
from scanner.status import _status_callback_supports_phase
from scanner.triage import _incremental_photo_index, _IncrementalTriage
from scanner.working_copies import _extract_working_copies

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable

    from db import Database
    from scanner.scan_types import CatalogFolderPath, ExifMetadata, FolderAliases, PhotoIdentity, StrPath

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ResolvedScanRoot:
    """The cataloged root a scan walks and the caller's inputs rebased under it.

    ``root_path`` is the catalog's spelling of the root; the path lists are
    the caller's ``ScanOptions`` values re-expressed under it, and
    ``frozen_files`` is the discovered-files manifest consumed into a list
    (None when the scan walks the filesystem).
    """

    root_path: Path
    restrict_dirs: Iterable[StrPath] | None
    restrict_files: Iterable[StrPath] | None
    skip_paths: Collection[StrPath] | None
    frozen_files: list[StrPath] | None


_EMPTY_SCAN_COUNTS = {
    "discovered": 0, "indexed": 0, "vanished": 0, "skipped_uncataloged": 0,
    "merged_companions": 0, "known_companions": 0,
}


def scan(
    root: StrPath, db: Database, *, options: ScanOptions | None = None,
    hooks: ScanHooks | None = None, control: ScanControl | None = None,
    counts: dict[str, int] | None = None,
    reported_photo_identities: dict[int, PhotoIdentity] | None = None,
) -> dict[str, int]:
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
    resolved = _resolve_scan_root(
        root, db, options.restrict_dirs, options.restrict_files,
        options.skip_paths, options.discovered_files,
    )
    if resolved is None:
        return counts
    from file_identity import catalog_folder_aliases, catalog_folder_path

    _ScanRun(
        root, resolved, db, counts,
        catalog_folder_path=catalog_folder_path,
        catalog_folder_aliases=catalog_folder_aliases,
        options=options,
        hooks=hooks,
        control=control,
        reported_photo_identities=reported_photo_identities,
    ).run()
    return counts


def _resolve_scan_root(
    root: StrPath, db: Database, restrict_dirs: Iterable[StrPath] | None,
    restrict_files: Iterable[StrPath] | None, skip_paths: Collection[StrPath] | None,
    discovered_files: Iterable[StrPath] | None,
) -> _ResolvedScanRoot | None:
    """The cataloged root and the caller's inputs rebased under it.

    Returns a ``_ResolvedScanRoot``, or ``None`` when the root must not be
    scanned (an other-app data bundle, or a missing root without a frozen
    manifest).
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
    # ``_ScanTally.log_summary``.
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
    return _ResolvedScanRoot(
        root_path, restrict_dirs, restrict_files, skip_paths, frozen_files,
    )


def _rebase_scan_inputs(
    original_root: str, root_path: Path, restrict_dirs: Iterable[StrPath] | None,
    restrict_files: Iterable[StrPath] | None, skip_paths: Iterable[StrPath] | None,
    discovered_files: Iterable[StrPath] | None,
) -> tuple[list[str] | None, list[str] | None, set[str] | None, list[Path] | None]:
    """Re-express the caller's paths under the cataloged root spelling."""
    def rebase_scan_path(path: StrPath) -> str:
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



class _ScanRun:
    """One ``scan()`` past its early exits.

    Walks the phases in order — discovery, the incremental pre-pass,
    batched metadata extraction, parallel hashing feeding the per-file
    catalog loop, then companion pairing and working copies. Each phase
    is a collaborator that owns the state only it mutates; the run hands
    each one the signals, the tally and the earlier phases' results it
    needs, and holds nothing else of its own.
    """

    def __init__(
        self, root: StrPath, resolved: _ResolvedScanRoot, db: Database, counts: dict[str, int], *,
        catalog_folder_path: CatalogFolderPath, catalog_folder_aliases: Callable[[Database], FolderAliases],
        options: ScanOptions, hooks: ScanHooks, control: ScanControl,
        reported_photo_identities: dict[int, PhotoIdentity] | None = None,
    ) -> None:
        self.root = root
        self.root_path = resolved.root_path
        self.db = db
        self.catalog_folder_path = catalog_folder_path
        self.catalog_folder_aliases = catalog_folder_aliases
        self.options = options
        # The rebased spellings ``_resolve_scan_root`` returned, not the
        # caller's ``options`` values.
        # Discovery and folder registration both traverse this scope. Retain
        # it once so a caller-provided generator is reusable by both passes.
        self.restrict_dirs = (
            None if resolved.restrict_dirs is None else tuple(resolved.restrict_dirs)
        )
        self.signals = _ScanSignals(
            control.cancel_check, hooks.status,
            _status_callback_supports_phase(hooks.status),
        )
        self.tally = _ScanTally(
            counts,
            progress_callback=hooks.progress,
            photo_callback=hooks.photo,
            photo_merged_callback=hooks.photo_merged,
            reported_identities=reported_photo_identities,
            sidecar_error_callback=hooks.sidecar_error,
        )
        self.discovery = _Discovery(
            root, resolved.root_path, resolved.frozen_files,
            signals=self.signals,
            restrict_dirs=self.restrict_dirs,
            restrict_files=resolved.restrict_files,
            skip_paths=resolved.skip_paths,
            recursive=options.recursive,
            permission_error_callback=hooks.permission_error,
        )
        self.hasher = _FeatureHasher(
            signals=self.signals,
            pause_check=control.pause_check,
            cancel_only_check=control.cancel_only_check,
        )

    def run(self) -> None:
        self.discovery.discover_files()
        self._count_discovered()
        self._load_catalog_state()
        files_to_process = self.triage.triage_files()
        paths_to_extract, metadata_map = self._extract_metadata(files_to_process)
        indexer = self._index_files(
            files_to_process, paths_to_extract, metadata_map,
        )
        self._finish_catalog(indexer.invalidated_photo_ids)
        self.tally.log_summary()

    def _count_discovered(self) -> None:
        excluded_frozen = self.discovery.excluded_frozen
        # Excluded frozen paths were part of the caller's promised manifest.
        # Account for them as vanished work so progress still reaches that
        # frozen denominator and import-in-place surfaces a partial-source
        # failure instead of silently succeeding on a smaller queue.
        self.tally.start(
            len(self.discovery.image_files) + excluded_frozen, excluded_frozen,
        )
        log.info("Found %d images in %s", self.tally.total, self.root)
        self.tally.report_progress()

    def _load_catalog_state(self) -> None:
        """Read what the pre-pass compares against and build its collaborators.

        The catalog reads run in a fixed order: the incremental baselines,
        the collection-change counter, then the folder aliases.
        """
        db = self.db
        options = self.options
        image_files = self.discovery.image_files
        existing_by_path = (
            _incremental_photo_index(db, image_files)
            if options.incremental else {}
        )
        known_companions = _KnownCompanions(db)
        # Cataloged rows the pre-pass sends for re-reading. The pre-pass
        # fills it; the companion keeper reads it in the per-file loop.
        reprocessed_row_ids = set()
        self.fills = _CompanionFills(db)
        self.collections = _CollectionMembership(db)
        self.folders = _ScanFolders(
            db, self.root_path,
            restrict_dirs=self.restrict_dirs,
            effective_restrict_dirs=self.discovery.effective_restrict_dirs,
            register_restrict_dirs_as_roots=options.register_restrict_dirs_as_roots,
            catalog_folder_path=self.catalog_folder_path,
            folder_aliases=self.catalog_folder_aliases(db),
        )
        self.keeper = _KnownCompanionKeeper(
            db,
            tally=self.tally,
            fills=self.fills,
            known_companions=known_companions,
            reprocessed_row_ids=reprocessed_row_ids,
            extract_full_metadata=options.extract_full_metadata,
        )
        self.triage = _IncrementalTriage(
            db, image_files,
            tally=self.tally,
            signals=self.signals,
            folders=self.folders,
            keeper=self.keeper,
            existing_by_path=existing_by_path,
            known_companions=known_companions,
            reprocessed_row_ids=reprocessed_row_ids,
            incremental=options.incremental,
            repair_missing_metadata=options.repair_missing_metadata,
        )

    # -- metadata ----------------------------------------------------

    def _extract_metadata(self, files_to_process: list[Path]) -> tuple[list[str], dict[str, ExifMetadata]]:
        """Batch-read ExifTool metadata.

        Returns the paths it was asked for and the metadata map.
        """
        # Batch extract metadata via ExifTool only for files that need processing
        paths_to_extract = [str(ip) for ip in files_to_process]
        if paths_to_extract and self.signals.status_callback:
            metadata_total = len(paths_to_extract)
            self.signals.emit_status(
                f"Extracting metadata (0 / {metadata_total} files)...",
                phase_current=0,
                phase_total=metadata_total,
                phase_label="Extracting metadata",
            )
        self.signals.check_cancelled()

        metadata_map = (
            extract_metadata(
                paths_to_extract,
                progress_callback=self._metadata_progress,
                checkpoint=self.signals.check_cancelled,
            )
            if paths_to_extract else {}
        )
        self.signals.check_cancelled()
        return paths_to_extract, metadata_map

    def _metadata_progress(self, current: int, total: int) -> None:
        self.signals.emit_status(
            f"Extracting metadata ({current} / {total} files)...",
            phase_current=current,
            phase_total=total,
            phase_label="Extracting metadata",
        )

    # -- main loop ---------------------------------------------------

    def _index_files(
        self, files_to_process: list[Path], paths_to_extract: list[str],
        metadata_map: dict[str, ExifMetadata],
    ) -> _FileIndexer:
        """Hash and catalog every file; returns the ``_FileIndexer`` that did."""
        options = self.options
        indexer = _FileIndexer(
            self.db, metadata_map,
            tally=self.tally,
            folders=self.folders,
            keeper=self.keeper,
            fills=self.fills,
            collections=self.collections,
            allow_photo_inserts=options.allow_photo_inserts,
            extract_full_metadata=options.extract_full_metadata,
            vireo_dir=options.vireo_dir,
            thumb_cache_dir=options.thumb_cache_dir,
        )
        groups = _CompanionGroups(
            self.db,
            tally=self.tally,
            keeper=self.keeper,
            catalog_file=indexer.catalog_file,
            extract_full_metadata=options.extract_full_metadata,
            vireo_dir=options.vireo_dir,
            thumb_cache_dir=options.thumb_cache_dir,
        )
        groups.track_companion_groups(files_to_process)
        try:
            for image_path, (phash, file_hash) in self.hasher.iter_features(
                files_to_process, paths_to_extract,
            ):
                waiting = indexer.index_file(image_path, phash, file_hash)
                if waiting is not None:
                    groups.wait_for_group(waiting)
                groups.mark_group_member_seen(image_path)
            groups.resolve_remaining_groups()
        except BaseException:
            # broad: any interruption, cancel included, marks the folders partial and re-raises.
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
            except Exception:  # broad: must not replace the scan failure re-raised below
                log.exception("Rollback after scan failure also failed")
            try:
                self.folders.update_folder_status("partial", only_from_partial=False)
            except Exception:  # broad: must not replace the scan failure re-raised below
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
                self.folders.update_folder_status("ok", only_from_partial=True)
            except Exception:  # broad: the scan succeeded; a stale partial flag only prompts a rescan
                log.exception("Failed to clear partial flag after scan success")
        return indexer

    # -- post-processing ---------------------------------------------

    def _finish_catalog(self, invalidated_photo_ids: set[int]) -> None:
        db = self.db
        vireo_dir = self.options.vireo_dir
        # Pair raw+JPEG companions: raw is primary, JPEG becomes companion_path.
        # Wrap post-processing so folder counts are always updated, even on failure.
        # On exception, roll back any uncommitted partial writes before updating
        # counts — otherwise folders.update_counts()'s commit would persist
        # half-applied pairing or working-copy records.
        try:
            # A JPEG merged into its RAW's row stops being its own photo, so
            # discount it: both files were counted on the way in but only the
            # RAW survives. Applied to the sink immediately (not just the
            # returned dict) so a caller reading counts after a later failure
            # in this block still sees the corrected number.
            self.fills.refill_owners_from_companions()
            _merged_ids = _pair_raw_jpeg_companions(
                db, vireo_dir=vireo_dir, thumb_cache_dir=self.options.thumb_cache_dir,
            )
            self.tally.discount_merged_companions(_merged_ids)
            self.tally.report_merged_photos(db, _merged_ids)

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
            if vireo_dir and not self.options.skip_working_copies:
                _extract_working_copies(
                    db, vireo_dir,
                    progress_callback=None,
                    status_callback=self.signals.status_callback,
                    scope=self.discovery.working_copy_scope(),
                    cancel_check=self.signals.cancel_check,
                )

            # Batched untracked-preview sweep. One os.listdir(previews/) for
            # the whole scan instead of one per invalidated photo — essential
            # when a rescan touches thousands of content-changed files.
            if invalidated_photo_ids:
                _sweep_untracked_previews_for_photos(
                    db, vireo_dir, invalidated_photo_ids,
                )
        except BaseException:
            db.rollback()
            raise
        finally:
            with db.transaction():
                db.folders.update_counts()
