"""The per-file catalog pass: one hashed file in, one up-to-date row out.

``_FileIndexer`` reads a file's pre-extracted metadata, inserts or
refreshes its row, writes its metadata and content-hash columns, drops
derived caches when the bytes changed, imports its keywords, and reports
it. A JPEG that may be a RAW's companion is handed back to wait for its
group instead (``scanner.companion_attach``). ``_CollectionMembership``
keeps a just-inserted photo out of collections that still list a deleted
photo's recycled id.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from image_loader import RAW_EXTENSIONS
from metadata import EXIF_SUMMARY_COLUMNS, exif_summary_columns
from PIL import Image
from preview_cache import (
    RecycledIdIndex,
    catalog_masks_directory,
    purge_cached_files_for_recycled_id,
)
from scanner.cache_invalidation import _invalidate_derived_caches
from scanner.companions import _COMPANION_JPEG_EXTENSIONS
from scanner.exif_fields import _extract_dimensions, _extract_timestamp
from scanner.hashing import EMPTY_FILE_SHA256
from scanner.keywords import _import_embedded_keywords_for_photo, _import_keywords_for_photo

if TYPE_CHECKING:
    from db import Database
    from scanner.companion_keep import _CompanionFills, _KnownCompanionKeeper
    from scanner.folders import _ScanFolders
    from scanner.run_state import _ScanTally
    from scanner.scan_types import ExifMetadata, StrPath

log = logging.getLogger(__name__)


@dataclass
class _FileMetadata:
    """What the main loop reads from one file's ExifTool payload."""

    file_meta: ExifMetadata
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


class _CollectionMembership:
    """Which photo ids static collections name, cached across inserts."""

    def __init__(self, db: Database) -> None:
        """Track collection edits on this connection, including rolled-back ones.

        data_version detects other connections' commits only. TEMP triggers
        notify a monotonic Python counter for this connection's collection
        writes; unlike a SQL counter, rollback cannot reuse a cache version.
        The triggers/function last only as long as the connection and never
        change the persistent catalog schema.
        """
        self.db = db
        self.db.scanning.install_collection_change_counter()
        self._collection_named_ids = None
        self._collection_named_ids_version = None

    def drop_inherited_collection_membership(self, photo_id: int) -> None:
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


class _FileIndexer:
    """Catalogs each hashed file; owns the caches it invalidated."""

    def __init__(
        self, db: Database, metadata_map: dict[str, ExifMetadata], *, tally: _ScanTally,
        folders: _ScanFolders, keeper: _KnownCompanionKeeper, fills: _CompanionFills,
        collections: _CollectionMembership, allow_photo_inserts: bool, extract_full_metadata: bool,
        vireo_dir: StrPath | None, thumb_cache_dir: StrPath | None,
    ) -> None:
        self.db = db
        self.metadata_map = metadata_map
        self.tally = tally
        self.folders = folders
        self.keeper = keeper
        self.fills = fills
        self.collections = collections
        self.allow_photo_inserts = allow_photo_inserts
        self.extract_full_metadata = extract_full_metadata
        self.vireo_dir = vireo_dir
        self.thumb_cache_dir = thumb_cache_dir
        # Photo IDs whose derived caches were invalidated this scan. Collected
        # so the untracked-preview sweep can run once as a batch instead of
        # per-photo (avoids O(N × M) directory walks on large rescans).
        self.invalidated_photo_ids: set[int] = set()
        # Photo IDs that already own cached derivative files, for the
        # recycled-rowid check on insert. Snapshotted once (lazily, on the
        # first insert) for the same batching reason as the sweep above.
        self.recycled_id_index = RecycledIdIndex(
            thumb_cache_dir or (
                os.path.join(vireo_dir, "thumbnails")
                if vireo_dir else ""
            ),
            vireo_dir=vireo_dir,
            masks_dir=catalog_masks_directory(db._db_path),
        )

    def index_file(self, image_path: Path, phash: str | None, file_hash: str | None) -> _IndexedFile | None:
        """Catalog one hashed file: row, metadata, caches, keywords.

        Returns the ``_IndexedFile`` when it is an uncataloged JPEG that
        must wait for the rest of its same-stem group, else ``None``.
        """
        db = self.db
        # File stats — first touch of the path in this loop. A file
        # deleted/renamed between discovery and here must skip, not
        # abort the scan and flag every folder in scope 'partial'.
        try:
            stat = image_path.stat()
        except OSError:
            log.info("File vanished during scan, skipping: %s", image_path)
            self.tally.skip_file("vanished")
            return None

        folder_id = self.folders.touch_folder(image_path.parent)
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

        if not row_already_existed and self.keeper.keep_known_companion(
            image_path, folder_id, file_size, file_mtime, file_hash, meta,
        ):
            return None

        # Process may refresh metadata for cataloged photos, but it must
        # never admit a filesystem path as a side effect. A row can
        # disappear between repair-scope resolution and this point; skip
        # that race rather than recreating it through add_photo().
        if not self.allow_photo_inserts and not row_already_existed:
            log.info(
                "Update-only scan skipped uncataloged file: %s", image_path,
            )
            self.tally.skip_file("skipped_uncataloged")
            return None

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
            # before it gets a row (see ``_CompanionGroups``).
            return item
        self.catalog_file(item)
        return None

    def catalog_file(self, item: _IndexedFile) -> None:
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

        self.tally.report_cataloged(
            photo_id, str(image_path), item.folder_id, item.image_path.name,
        )

    def _read_file_metadata(self, image_path: Path) -> _FileMetadata:
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
                except Exception:  # broad: rawpy raises undocumented errors; dimensions stay unknown
                    log.debug("Could not read RAW dimensions from %s", image_path, exc_info=True)
            else:
                try:
                    with Image.open(str(image_path)) as img:
                        width, height = img.size
                except Exception:  # broad: PIL raises a wide set on bad files; dimensions stay unknown
                    log.debug("Could not read dimensions from %s", image_path, exc_info=True)

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

    def _add_photo(self, item: _IndexedFile) -> int:
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
                self.collections.drop_inherited_collection_membership(photo_id)
        # Credit the row after the insert and cleanup are durable. Later
        # cache invalidation or callbacks may fail without losing this count.
        self.tally.credit_indexed(photo_id)

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

    def _write_photo_columns(self, photo_id: int, item: _IndexedFile) -> None:
        # Update metadata columns (also fixes existing photos that were
        # inserted before ExifTool metadata was available)
        meta = item.meta
        file_meta = meta.file_meta
        file_hash = item.file_hash
        file_size = item.file_size
        row_already_existed = item.row_already_existed
        updates, update_params = self._identity_column_updates(item)
        pending_companion_fill = self.fills.take_pending_companion_fill(
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
                # ``_CompanionFills``; if the scan is
                # interrupted first, null the companion's stored
                # ``file_mtime`` so the next incremental scan sees a
                # stale stat and re-reads the companion, replaying the
                # fill. Harmless no-op when the row has no
                # ``companion_identities`` entry (UPDATE touches 0 rows).
                self.db.companion_pairing.clear_identity_mtime(photo_id)
            self.db.commit_with_retry()

    def _identity_column_updates(self, item: _IndexedFile) -> tuple[list[str], list[object]]:
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

    def _heal_changed_content(self, photo_id: int, item: _IndexedFile) -> None:
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
