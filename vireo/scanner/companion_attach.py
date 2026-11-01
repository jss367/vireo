"""Pairing a JPEG with its RAW before the JPEG gets a row.

A camera JPEG beside its RAW used to be inserted as its own photo and
merged into the RAW by the end-of-scan pairing pass. Between the two
its id was live: ``photo_callback`` reported it, so the pipeline's
and import-in-place's collections, thumbnail queue and counts kept a
deleted id, and because ``photos`` has no AUTOINCREMENT the next
insert could take that id and join those collections. The scan now
makes the pairing decision first and records a paired JPEG straight
onto its RAW's row, so the transient row never exists.

``_CompanionGroups`` owns the same-stem groups the per-file loop is
waiting on, decides each group once all its files have been seen, and
attaches and publishes the JPEGs it pairs.
"""

from __future__ import annotations

import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from metadata import exif_summary_columns
from scanner.companion_keep import _apply_companion_gap_fill, _companion_columns, _lock_companion_owner
from scanner.companions import (
    _COMPANION_JPEG_EXTENSIONS,
    _COMPANION_RAW_EXTENSIONS,
    _companions_compatible,
    _defer_primary_display_cache_invalidation,
    _pairing_rows_for_stem,
    _pick_compatible_raw_jpeg_pairs,
    _run_post_commit_fs_actions,
)
from scanner.keywords import _import_embedded_keywords_for_photo

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from db import Database
    from scanner.companion_keep import _KnownCompanionKeeper
    from scanner.indexing import _IndexedFile
    from scanner.run_state import _ScanTally
    from scanner.scan_types import CompanionOwnership, PairingRow, StrPath

log = logging.getLogger(__name__)


class _CompanionAttachRefused(Exception):
    """Rolls back a row-less companion attach whose owner changed."""


def _companion_group_key(image_path: Path) -> tuple[str, str]:
    """``(directory, stem)``: the files pairing considers together."""
    return (str(image_path.parent), image_path.stem)


class _CompanionGroups:
    """Same-stem groups whose uncataloged JPEGs wait for their RAW."""

    def __init__(
        self, db: Database, *, tally: _ScanTally, keeper: _KnownCompanionKeeper,
        catalog_file: Callable[[_IndexedFile], None], extract_full_metadata: bool,
        vireo_dir: StrPath | None, thumb_cache_dir: StrPath | None,
    ) -> None:
        self.db = db
        self.tally = tally
        self.keeper = keeper
        # ``_FileIndexer.catalog_file``: the ordinary path for a JPEG that
        # turns out to be a photo of its own.
        self.catalog_file = catalog_file
        self.extract_full_metadata = extract_full_metadata
        self.vireo_dir = vireo_dir
        self.thumb_cache_dir = thumb_cache_dir
        self._unseen_group_members = defaultdict(int)
        self._waiting_companions = defaultdict(list)

    def track_companion_groups(self, files_to_process: Iterable[Path]) -> None:
        """Count each same-stem group's RAW/JPEG files in this scan.

        A JPEG waits until the loop has seen every file of its group, so
        its RAW (which may sort after it: ``IMG.jpg`` < ``IMG.nef``) has a
        row with fresh metadata, and a group's JPEGs are decided together
        the way the end-of-scan pass would decide them.
        """
        self._unseen_group_members = defaultdict(int)
        self._waiting_companions = defaultdict(list)
        for image_path in files_to_process:
            ext = image_path.suffix.lower()
            if (
                ext in _COMPANION_RAW_EXTENSIONS
                or ext in _COMPANION_JPEG_EXTENSIONS
            ):
                self._unseen_group_members[
                    _companion_group_key(image_path)
                ] += 1

    def wait_for_group(self, item: _IndexedFile) -> None:
        """Hold an uncataloged JPEG until its group has been seen."""
        # Decide whether this JPEG is a photo or a RAW's companion
        # before it gets a row (see ``resolve_companion_group``).
        self._waiting_companions[_companion_group_key(item.image_path)].append(
            item,
        )

    def mark_group_member_seen(self, image_path: Path) -> None:
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
            self.resolve_companion_group(key)

    def resolve_remaining_groups(self) -> None:
        # Every group's last member has been seen by now; this only
        # matters if a path appeared in ``files_to_process`` twice.
        for key in list(self._waiting_companions):
            self.resolve_companion_group(key)

    def resolve_companion_group(self, key: tuple[str, str]) -> None:
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
            self.catalog_file(item)

    def _uncataloged_member(self, item: _IndexedFile) -> PairingRow:
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

    def _attach_companion(self, item: _IndexedFile, owner: PairingRow) -> bool:
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
            with db.transaction():
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
                # ``resolve_companion_group`` reading its timestamp/camera
                # columns and this guarded UPDATE. Without this re-check the
                # pairing would commit on a stale compatibility decision and
                # permanently attach unrelated files.
                if not self._owner_still_compatible(owner_id, item):
                    raise _CompanionAttachRefused
                db.companion_pairing.record_identity(
                    owner_id, name, item.file_size, item.meta.timestamp,
                    item.file_hash, item.file_mtime,
                )
                _apply_companion_gap_fill(
                    db, owner_id,
                    _companion_columns(item.meta, self.extract_full_metadata),
                    ownership,
                )
                _import_embedded_keywords_for_photo(
                    db, owner_id, item.meta.file_meta,
                )
                self.keeper.import_companion_sidecar(
                    image_path, {"owner_id": owner_id},
                )
                self.tally.credit_merged_companion()
        except _CompanionAttachRefused:
            # The unit of work rolled back already; the refusals precede
            # every other write.
            return False
        self._publish_attached_companion(image_path, owner, ownership)
        self.tally.report_progress()
        return True

    def _owner_still_compatible(self, owner_id: int, item: _IndexedFile) -> bool:
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

    def _publish_attached_companion(self, image_path: Path, owner: PairingRow, ownership: CompanionOwnership) -> None:
        """Drop the RAW's display caches and report the committed attach."""
        owner_id = owner["id"]
        photo_callback = self.tally.photo_callback
        reported_identities = self.tally.reported_identities
        # Report AFTER the unit of work commits: the thumbnail worker runs
        # on a separate connection and only sees committed data. If the
        # callback fired inside the guard, a queue entry whose canonical
        # path falls back to the companion (missing RAW) would be checked
        # against a row where ``companion_path`` is still NULL and
        # ``_still_owns`` would drop it. Unlike ``finish_known_companion``,
        # which handles an unchanged companion whose ``companion_path`` was
        # committed by an earlier scan, this attach is the write that first
        # makes the pairing visible.
        #
        # Invalidate the RAW's display cache before the callback: a
        # callback that raises leaves the pairing committed, and the next
        # incremental scan takes ``finish_known_companion`` instead,
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
        if photo_callback:
            # Revalidate ownership before publishing ``owner_id``. The
            # attach commit was released by the caller so the post-commit FS
            # actions could see it; in that window another connection can
            # delete the RAW and SQLite can reuse its rowid for an
            # unrelated photo. Without this check, an incremental scan
            # that processes only a new JPEG beside an unchanged
            # cataloged RAW would publish the stale id to the pipeline
            # or import collection — and no end-of-scan merge callback
            # would correct it because the JPEG never had a row of its
            # own. Taking the writer lock inside a unit of work makes
            # the check-then-publish atomic against concurrent deletes.
            prepare = getattr(photo_callback, "prepare_publication", None)
            publication = (
                prepare(owner_id, str(image_path)) if prepare is not None
                else contextlib.nullcontext(
                    lambda: photo_callback(owner_id, str(image_path)),
                )
            )
            # A streaming receiver waits for Pause and queue capacity before
            # the writer lock, then publishes through a nonblocking callback.
            # Ownership is revalidated AFTER that wait, keeping rowid reuse
            # protection without preventing the consumer's thumbnail commits.
            with publication as publish, self.db.transaction():
                owner_still_valid = _lock_companion_owner(
                    self.db, owner_id, ownership,
                )
                if owner_still_valid:
                    reported_identities[owner_id] = (
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
            reported_identities[owner_id] = (
                owner["folder_id"], owner["filename"],
            )
