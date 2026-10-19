"""The incremental pre-pass: which discovered files need full processing.

``_IncrementalTriage`` stats every discovered file, credits a cataloged
row that still vouches for its file, re-imports a changed sidecar inline,
and settles companion JPEGs the catalog already holds — returning only
the files the metadata, hashing and per-file passes must handle.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from image_loader import RAW_EXTENSIONS
from scanner.companions import _companion_stat_unchanged
from scanner.hashing import EMPTY_FILE_SHA256
from scanner.keywords import _import_keywords_for_photo

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from db import Database
    from scanner.companion_keep import _KnownCompanionKeeper
    from scanner.companions import _KnownCompanions
    from scanner.folders import _ScanFolders
    from scanner.run_state import _ScanSignals, _ScanTally

log = logging.getLogger(__name__)


def _incremental_photo_index(db: Database, image_files: list[Path]) -> dict[str, sqlite3.Row]:
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


class _IncrementalTriage:
    """The pre-pass between discovery and metadata extraction."""

    def __init__(
        self, db: Database, image_files: list[Path], *, tally: _ScanTally, signals: _ScanSignals,
        folders: _ScanFolders, keeper: _KnownCompanionKeeper,
        existing_by_path: dict[str, sqlite3.Row], known_companions: _KnownCompanions,
        reprocessed_row_ids: set[int], incremental: bool, repair_missing_metadata: bool,
    ) -> None:
        self.db = db
        self.image_files = image_files
        self.tally = tally
        self.signals = signals
        self.folders = folders
        self.keeper = keeper
        self.existing_by_path = existing_by_path
        self.known_companions = known_companions
        # Cataloged rows the pre-pass sends for re-reading.
        self.reprocessed_row_ids = reprocessed_row_ids
        self.incremental = incremental
        self.repair_missing_metadata = repair_missing_metadata
        # Companion JPEGs the incremental pre-pass found unchanged by stat,
        # as ``(image_path, owner_id)``; settled once the pre-pass knows
        # which RAW rows it is re-reading (see ``_settle_stat_unchanged_companions``).
        self.stat_unchanged_companions = []

    def triage_files(self) -> list[Path]:
        """Return the files that need full processing."""
        # First pass: determine which files need full processing (for incremental mode).
        # Handle XMP-only changes inline; collect files needing metadata extraction.
        files_to_process = []
        try:
            self.folders.register_scan_targets()
            for image_path in self.image_files:
                self.signals.check_cancelled()
                if self._needs_processing(image_path):
                    files_to_process.append(image_path)
            if self._settle_stat_unchanged_companions(files_to_process):
                files_to_process.sort()
        except BaseException:
            # broad: any interruption, cancel included, marks the folders partial and re-raises.
            # Pre-pass died (e.g. non-retryable DB error on an XMP commit).
            # Route through the same partial-status path as a main-loop failure
            # so users see the badge and can rescan.
            try:
                self.db.rollback()
            except Exception:  # broad: must not replace the pre-pass failure re-raised below
                log.exception("Rollback after pre-pass failure also failed")
            try:
                self.folders.update_folder_status("partial", only_from_partial=False)
            except Exception:  # broad: must not replace the pre-pass failure re-raised below
                log.exception("Failed to flag folders partial after pre-pass failure")
            raise
        return files_to_process

    def _needs_processing(self, image_path: Path) -> bool:
        """``False`` when the pre-pass already disposed of ``image_path``."""
        try:
            stat = image_path.stat()
        except OSError:
            # File deleted/renamed between discovery and this pass —
            # skip it instead of aborting the whole scan (the discovery
            # walk has the same guard for broken symlinks).
            log.info("File vanished during scan, skipping: %s", image_path)
            self.tally.skip_file("vanished")
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

    def _settle_stat_unchanged_companions(self, files_to_process: list[Path]) -> bool:
        """Credit unchanged companions, or re-read them beside a re-read RAW.

        Re-reading a RAW rewrites its EXIF summary columns from the RAW
        alone, clearing the ones only the camera JPEG carries. Pairing
        filled those from the JPEG, so a companion whose RAW is being
        re-read is read too, and its columns refill the gaps (see
        ``_CompanionFills.refill_owners_from_companions``). Returns whether
        any were added to ``files_to_process``.

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
                if not self.keeper.finish_known_companion(image_path, known):
                    # The cached owner vanished or changed. Hash and
                    # catalog the JPEG against fresh state rather than
                    # losing its disposition or crediting a recycled ID.
                    files_to_process.append(image_path)
                    added = True
        self.stat_unchanged_companions = []
        return added

    def _reuse_existing_row(
        self, existing: sqlite3.Row, full_path_str: str, stat: os.stat_result, xmp_path: Path,
        xmp_mtime: float | None,
    ) -> bool:
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
            self.tally.credit_reused_row(
                existing["id"], full_path_str,
                existing["folder_id"], existing["filename"],
            )
            return True

        # XMP changed: re-import keywords
        if not xmp_unchanged and xmp_mtime is not None:
            if _import_keywords_for_photo(db, existing["id"], str(xmp_path)):
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
            self.tally.credit_reused_row(
                existing["id"], full_path_str,
                existing["folder_id"], existing["filename"],
            )
            return True
        return False
