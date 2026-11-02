"""Companion JPEGs the catalog already holds on their RAW's row.

``_KnownCompanionKeeper`` leaves an unchanged companion in place (sidecar
keywords, refreshed identity, credit to its RAW) while the owner row stays
locked. ``_CompanionFills`` owns the gap fills a companion queues for its
RAW: the RAW's re-read consumes them in its own UPDATE, and the end of the
scan replays any that are left.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from canonical_schema import EXIF_SUMMARY_COLUMNS
from metadata import exif_summary_columns
from scanner.companions import (
    _companion_bytes_unchanged,
    _fill_primary_metadata_gaps,
    _read_metadata_transfer_rows,
)
from scanner.keywords import (
    UNREADABLE_SIDECAR_REASON,
    _import_embedded_keywords_for_photo,
    _import_keywords_for_photo,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from db import Database
    from scanner.companions import _KnownCompanions
    from scanner.indexing import _FileMetadata
    from scanner.run_state import _ScanTally
    from scanner.scan_types import ColumnRow, CompanionOwnership

log = logging.getLogger(__name__)


def _lock_companion_owner(db: Database, owner_id: int, ownership: CompanionOwnership) -> bool:
    """Revalidate ownership while acquiring the transaction's writer lock.

    Photo IDs can be recycled between commits. The guarded no-op write
    prevents a concurrent deletion/replacement between this check and
    the metadata write; the caller's commit releases the lock.
    """
    folder_id, filename, companion = ownership
    return db.companion_pairing.lock_owner(
        owner_id, folder_id, filename, companion,
    )


def _apply_companion_gap_fill(
    db: Database, owner_id: int, companion_columns: dict[str, Any], ownership: CompanionOwnership,
) -> None:
    """Fill the owner's gaps from the companion in the current transaction.

    Reads the owner's columns as they stand right now (so a prior
    clear-to-NULL in this transaction is visible) and runs
    ``_fill_primary_metadata_gaps`` without committing. The caller
    owns the commit, so the fill lands atomically with whatever
    other work the caller is already staging — see
    ``_KnownCompanionKeeper._keep_locked_companion`` and
    ``_FileIndexer._write_photo_columns``.
    """
    if not _lock_companion_owner(db, owner_id, ownership):
        return
    owner_columns, _ = _read_metadata_transfer_rows(
        db, {"id": owner_id}, {"id": owner_id},
    )
    if owner_columns is None:
        return
    _fill_primary_metadata_gaps(
        db, {"id": owner_id}, owner_columns, companion_columns,
    )


def _companion_columns(meta: _FileMetadata, extract_full_metadata: bool) -> dict[str, Any]:
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
            json.dumps(file_meta) if extract_full_metadata else "{}"
        ),
        "width": meta.width,
        "height": meta.height,
    }
    summary = exif_summary_columns(file_meta)
    for column in EXIF_SUMMARY_COLUMNS:
        columns[column] = summary.get(column)
    return columns


class _CompanionFills:
    """Gap fills queued for a RAW by its unchanged companion.

    ``owner_id -> (ownership, companion columns)`` for companions found
    unchanged after their metadata was read. Populated as each companion
    is processed; drained by ``take_pending_companion_fill`` when the
    owner is re-read (so the gap fill lands in the same UPDATE as the
    clear-to-NULL — see ``_FileIndexer._write_photo_columns``), and by the
    end-of-scan safety net ``refill_owners_from_companions``.
    """

    def __init__(self, db: Database) -> None:
        self.db = db
        self.pending = {}

    def queue(self, owner_id: int, ownership: CompanionOwnership, companion_columns: dict[str, Any]) -> None:
        self.pending[owner_id] = (ownership, companion_columns)

    def take_pending_companion_fill(self, photo_id: int, row_already_existed: bool) -> dict[str, Any] | None:
        """The companion fill queued for this owner, if it still owns it."""
        pending_companion = (
            self.pending.pop(photo_id, None)
            if row_already_existed else None
        )
        if pending_companion is None:
            return None
        ownership, columns = pending_companion
        if _lock_companion_owner(self.db, photo_id, ownership):
            return columns
        return None

    def refill_owners_from_companions(self) -> None:
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
        if not self.pending:
            return
        for owner_id, (ownership, columns) in self.pending.items():
            _apply_companion_gap_fill(self.db, owner_id, columns, ownership)
        self.db.commit_with_retry()
        self.pending = {}


class _KnownCompanionKeeper:
    """Leaves companions the catalog already pairs on their RAW's row."""

    def __init__(
        self, db: Database, *, tally: _ScanTally, fills: _CompanionFills,
        known_companions: _KnownCompanions, reprocessed_row_ids: set[int],
        extract_full_metadata: bool,
    ) -> None:
        self.db = db
        self.tally = tally
        self.fills = fills
        self.known_companions = known_companions
        # Cataloged rows the incremental pre-pass sends for re-reading;
        # filled by ``_IncrementalTriage``, read here.
        self.reprocessed_row_ids = reprocessed_row_ids
        self.extract_full_metadata = extract_full_metadata

    def finish_known_companion(self, image_path: Path, known: sqlite3.Row) -> bool:
        """Import sidecar and report the path while its owner stays locked.

        One unit of work from the owner check through the tagging and the
        credit, so nothing releases ownership in between. The full/hash path
        uses the same guard as stat settling.
        """
        ownership = (
            known["folder_id"], known["owner_filename"], image_path.name,
        )
        with self.db.transaction():
            if not _lock_companion_owner(self.db, known["owner_id"], ownership):
                return False
            self.import_companion_sidecar(image_path, known)
            self.tally.credit_known_companion(
                known["owner_id"], str(image_path),
                known["folder_id"], known["owner_filename"],
            )
        return True

    def import_companion_sidecar(self, image_path: Path, known: ColumnRow) -> None:
        """Import the validated companion's additive sidecar keywords.

        Import shared-stem XMP too: the RAW can vanish after the JPEG is
        handled, even if it existed at discovery or at this call. Repeating
        the keyword import on the same owner is idempotent and preserves
        pending removals, just as the old insert-and-merge path did. Runs in
        the caller's unit of work, which commits the imported keywords.
        """
        xmp_path = image_path.with_suffix(".xmp")
        if not xmp_path.exists():
            return
        try:
            imported = _import_keywords_for_photo(
                self.db, known["owner_id"], str(xmp_path),
            )
        except Exception as exc:  # broad: XMP parsing raises undocumented errors; one bad sidecar must not stop the scan, and the tally reports it
            log.exception(
                "Failed to import companion sidecar %s for owner %s",
                xmp_path, known["owner_id"],
            )
            self.tally.report_sidecar_failure(
                str(xmp_path), image_path.name, str(exc) or type(exc).__name__,
            )
            return
        if not imported:
            # Unreadable: nothing was imported. The companion is re-read,
            # and this retried, on the next scan.
            self.tally.report_sidecar_failure(
                str(xmp_path), image_path.name, UNREADABLE_SIDECAR_REASON,
            )

    def keep_known_companion(
        self, image_path: Path, folder_id: int, file_size: int, file_mtime: float,
        file_hash: str | None, meta: _FileMetadata,
    ) -> bool:
        """Leave an unchanged companion JPEG in its RAW's row; True if it was.

        A companion whose bytes changed (or that has no stored identity to
        compare) returns False and is re-paired with its RAW without a row
        of its own (``_CompanionGroups.resolve_companion_group`` ->
        ``_attach_companion``), which refreshes the stored identity, fills
        the RAW's gaps from it, carries its new keywords over, and drops the
        RAW's companion-derived caches.
        """
        known = self.known_companions.lookup(folder_id, image_path.name)
        if known is None:
            return False
        ownership = (folder_id, known["owner_filename"], image_path.name)
        with self.db.transaction():
            if not _lock_companion_owner(self.db, known["owner_id"], ownership):
                return False
            return self._keep_locked_companion(
                image_path, known, file_size, file_mtime, file_hash, meta,
            )

    def _keep_locked_companion(
        self, image_path: Path, known: sqlite3.Row, file_size: int, file_mtime: float,
        file_hash: str | None, meta: _FileMetadata,
    ) -> bool:
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
                companion_columns = _companion_columns(
                    meta, self.extract_full_metadata,
                )
                # Apply the gap fill inline in this same transaction as
                # the file_mtime update: a successful commit means both
                # are durable, a rollback means neither. Handles the
                # common case where the owner RAW was already re-read
                # earlier this scan (its JPEG-only columns are already
                # cleared, so this UPDATE fills them). When the companion
                # comes first the fill is a no-op here (columns not
                # cleared yet) and ``_write_photo_columns`` will fold
                # the same fill into the owner's clear-to-NULL UPDATE
                # via ``_CompanionFills`` below.
                _apply_companion_gap_fill(
                    self.db, known["owner_id"], companion_columns,
                    (known["folder_id"], known["owner_filename"], image_path.name),
                )
                self.fills.queue(
                    known["owner_id"],
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
        else:
            return False
        return self.finish_known_companion(image_path, known)
