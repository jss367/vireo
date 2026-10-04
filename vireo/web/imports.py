"""HTTP adapters for import previews, readiness, and job submission.

The archive and in-place job workflows live in ``services.imports.ImportService``.
Per-app callbacks and live settings are injected by the blueprint factory.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import source_discovery
from db import Database, _chunks
from flask import Blueprint, Response, abort, jsonify, make_response, request
from jobs import describe_jobs
from metadata import scan_metadata_warning
from new_images import invalidate_new_images_after_scan
from services.imports import ImportFailure, ImportService
from services.local_folder import (
    local_copy_scan_conflict,
    stage_pending_source_paths,
)
from services.local_workspace import stage_boundary_lock
from services.move_steps import FOLDER_MOVE_PHASES, MoveSteps, folder_move_steps
from services.startup_tasks import metadata_repair_count
from web.background_jobs import make_background_job
from web.request_args import reject_visual_collection

log = logging.getLogger(__name__)



# Adaptive flush cadence for the duplicate-check SSE stream. Byte-for-byte
# hashes on slow cards spend far more than this per file, so each check ends
# its own event and cancellation is bounded by one file. Metadata-only
# checks on a 100k-file source blow through hundreds of files inside one
# window and coalesce naturally, keeping event count in the low hundreds
# instead of one-per-file. Module-level so tests can override it.
DUPLICATE_CHECK_FLUSH_INTERVAL_SECONDS = 0.1

# Chunk size for the duplicate-check prep phase (EXIF batching and, in
# verify_by_hash + recovery mode, source_capture_timestamps for folder
# planning). Small enough that a superseded browser request cancels within
# one chunk's worth of I/O — otherwise a card with tens of thousands of
# files spends the entire prep phase reading metadata before the WSGI layer
# ever gets a chance to observe the disconnect. Aligned with
# metadata._BATCH_SIZE so an outer chunk maps to at most one ExifTool
# subprocess for its leftovers, keeping subprocess overhead unchanged.
# Module-level so tests can override it.
DUPLICATE_CHECK_PREP_BATCH_SIZE = 100


def _folder_ids_under(folders, staging_destination):
    """Return the ids of ``folders`` that live inside ``staging_destination``.

    Browse badges the sidebar rows for a pending NAS transfer, and the only
    thing tying a catalog folder to that transfer is its path: an import that
    was kept locally registers its staging tree as ordinary folders, so the
    sidebar otherwise shows a bare date leaf (``12``) with nothing saying the
    files are still in Vireo's staging directory. The staging root itself is
    included — it is a real folder row when the import wrote photos there.
    """
    root = os.path.normpath(staging_destination) if staging_destination else ""
    # A missing or filesystem-root staging path would badge the whole tree.
    if root in ("", ".", os.sep, os.path.splitdrive(root)[0] + os.sep):
        return []
    prefix = root + os.sep
    ids = []
    for folder in folders:
        path = os.path.normpath(folder["path"]) if folder["path"] else ""
        if path == root or path.startswith(prefix):
            ids.append(folder["id"])
    return ids


def _all_folders(db):
    """Every catalog folder, not just the active workspace's.

    A staging tree can be unlinked from the workspace that imported it while
    still linked to a sibling -- nothing guards ``pending_archives`` against
    folder removal -- and the rows, the files and the queued edits all survive
    that. Scoping by workspace membership would report nothing to sync and
    hide the button, which reads as "nothing to do here" rather than "I cannot
    see this tree". A transfer is defined by a path on disk, so resolve it
    from the path.
    """
    return db.conn.execute("SELECT id, path FROM folders").fetchall()


def _staging_folder_ids(db, staging_destination):
    """``_folder_ids_under`` for one archive, reading the folder list itself.

    The listing route hoists the folder read across every pending row; a job
    thread has exactly one archive and its own db, so it asks directly rather
    than carrying the hoisted list across threads.
    """
    return _folder_ids_under(_all_folders(db), staging_destination)


# How many times the pre-transfer sync re-reads the queue before giving up on
# draining it. Job admission is workspace-scoped and the rating/keyword routes
# are ordinary requests, so the user can queue an edit while the sync runs. A
# second pass catches that; the cap only exists so a change that somehow never
# clears cannot spin here forever. Whatever is still queued after the move is
# reported rather than silently swallowed -- see ``_residual_staged_changes``.
_MAX_SYNC_DRAIN_PASSES = 5


def _sync_staged_metadata(db, progress, sync_job_lock, folder_ids):
    """Write the staged import's queued sidecar edits, before it leaves.

    Runs against the local staging copy on purpose. A verified transfer
    deletes the originals, so from then on the same sync has to write over the
    NAS connection -- and only while that mount is up. Returns
    ``(photos_synced, undeliverable_identities)``; raises so the caller abandons
    the transfer when a write actually fails, because the user asked for both
    halves and silently sending stale sidecars to the NAS is the outcome they
    were trying to avoid.

    Held under the same app-wide lock as ``/api/jobs/sync``: that job is not
    excluded by this one's workspace-scoped admission, and ``sync_to_xmp``
    takes its sidecar locks per call, so the two could otherwise
    read-modify-write one .xmp concurrently. Acquired cancellably, because the
    wait can be long and nothing on disk has been touched yet.

    The queue is re-read after every pass so an edit made while the sync was
    running still travels with the transfer.

    ``create_missing_sidecars`` is on here and nowhere else: this is the last
    moment a rating-only photo can get a sidecar at all, because the transfer
    is about to delete the file it would sit next to.

    ``folder_paths`` is built from every catalog folder rather than from the
    active workspace's tree so the sync resolves sidecar paths even when the
    staging tree has been unlinked from its owning workspace. Membership is
    not what defines the transfer -- the path is -- and the workspace-scoped
    map would otherwise fail every photo as "folder not accessible". The
    same shape drives ``require_workspace_membership=False``: an unlinked
    staged photo's assigned-location lookup would otherwise raise "photo
    not in workspace" and drop every queued ``location`` change on the
    floor, so that check is skipped for the pre-transfer sync too.
    """
    import sync as sync_mod

    def sync_progress(current, total):
        progress(current, total, "", "Writing metadata to sidecars")

    folder_paths = {row["id"]: row["path"] for row in _all_folders(db)}
    synced_photos = set()
    # Changes this workspace declines to write to XMP (a flag under
    # sync_flags_to_xmp off). Tracked so the drain stops instead of re-running
    # them forever, and so the residual count below does not report them as
    # edits that "missed the transfer" -- a different claim, and a false one.
    # Keyed on row identity, never on the row id; see ``staged_sync_scope``.
    undeliverable = set()
    for _ in range(_MAX_SYNC_DRAIN_PASSES):
        changes, _here, _elsewhere, _overlap = db.staged_sync_scope(folder_ids)
        pending = [entry for entry in changes if entry[0] not in undeliverable]
        if not pending:
            break
        result = sync_mod.sync_to_xmp(
            db, progress_callback=sync_progress,
            change_ids=[cid for _key, cid, _pid in pending],
            create_missing_sidecars=True,
            folder_paths=folder_paths,
            # Same reason ``folder_paths`` is passed: the staging tree may
            # have been unlinked from the workspace that imported it, and
            # the default workspace-membership verification on
            # ``get_assigned_photo_location`` would then refuse every
            # queued ``location`` change for those photos even though the
            # sidecar path resolves fine.
            require_workspace_membership=False,
        )
        # A change this workspace declines to write to XMP is reported as a
        # failure so the ordinary sync job lands in history as "failed". It is
        # not a reason to abandon a transfer: nothing went wrong, and the edit
        # was never bound for the sidecar. Only failures carrying a ``reason``
        # -- the ones an actual prepare or write raises -- stop the move.
        if [f for f in result["failures"] if f.get("reason")]:
            raise ValueError(
                "Metadata sync failed, so nothing was sent: "
                + "; ".join(result["errors"])
                + ". The local originals are untouched. Fix the sidecars and "
                "try again, or use Send to NAS to transfer without syncing first."
            )
        after, _here, _elsewhere, _overlap = db.staged_sync_scope(folder_ids)
        remaining = {key for key, _cid, _pid in after}
        # Counted as photos, not as per-pass tallies: one photo edited across
        # two passes is one sidecar, and the banner counts distinct photos too.
        synced_photos.update(
            pid for key, _cid, pid in pending if key not in remaining)
        undeliverable.update(
            key for key, _cid, _pid in pending if key in remaining)
    return len(synced_photos), undeliverable


def _staged_photo_ids(db, folder_ids):
    """Every photo id currently in the given staging folders.

    Captured before ``send_pending_archive`` so the post-transfer residual
    check can still find changes queued during the copy even when a
    tracked-merge move reparents the photos onto the destination folder
    ids -- at which point a folder-id-scoped re-read would report zero and
    the completed job would falsely claim no metadata missed the transfer.
    A photo's id survives the reparent; its folder_id does not.
    """
    if not folder_ids:
        return []
    ids = []
    for chunk in _chunks(folder_ids):
        placeholders = ",".join("?" for _ in chunk)
        ids.extend(row["id"] for row in db.conn.execute(
            f"SELECT id FROM photos WHERE folder_id IN ({placeholders})",
            list(chunk),
        ))
    return ids


def _residual_staged_changes(db, photo_ids, undeliverable):
    """Count edits queued too late to have travelled with the transfer.

    Blocking the rating and keyword routes for the length of a NAS transfer
    would be a worse trade than this gap, so the gap is reported instead of
    closed: the job must not claim it synced everything when an edit landed
    after the last drain. Only ids the sync never saw count -- something it
    considered and left queued was deliberately not written to XMP (a flag
    under sync_flags_to_xmp off), which is a different claim than "missed the
    transfer".

    Scoped by photo id, not folder id: ``send_pending_archive`` takes the
    tracked-merge path when the NAS destination is already represented in
    the catalog, and that reparents each staged photo onto the existing
    destination folder id. A folder-id-scoped re-read would then find
    nothing even if the user queued an edit during the copy. Photo ids are
    captured before the move for this reason.

    Returns ``None`` when the re-read itself fails. The transfer has already
    succeeded by then, so the failure is not fatal -- but an affirmative
    ``0`` would be read as "nothing missed the transfer" when the honest
    answer is "I could not tell", and the edits are still sitting in the
    queue. The caller says so in the summary.
    """
    try:
        changes, _here, _elsewhere, _overlap = db.staged_sync_scope_by_photos(photo_ids)
        return sum(1 for key, _cid, _pid in changes if key not in undeliverable)
    except Exception:
        log.warning("Could not re-check the sync queue after a NAS transfer", exc_info=True)
        return None


@dataclass
class _RecoverySource:
    """One source file checked against its planned destination's
    candidates by ``_DuplicateCheckStream._recovery_candidate``."""

    source_file: Path
    compute_file_hash: Callable
    src_hash_cache: list = field(default_factory=list)

    def _src_hash(self):
        if not self.src_hash_cache:
            try:
                self.src_hash_cache.append(self.compute_file_hash(
                    str(self.source_file)))
            except OSError:
                self.src_hash_cache.append(None)
        return self.src_hash_cache[0]

    def _is_source(self, cand_path):
        """Reject destination candidates that ARE the source file itself
        — the run rejects that self-copy overlap (destination is an
        ancestor of the source AND the folder template renders back onto
        the source folder, e.g. importing /archive/2026/2026-07-03/IMG.jpg
        into /archive with %Y/%Y-%m-%d) rather than adopting it, so the
        preview must not promise "verified & adopted, not re-copied" and
        subtract it from "to copy" for a file the run will fail. Mirrors
        import_job's samefile guard with the same normalized-path fallback
        for paths that can't be stat'd."""
        try:
            return (
                os.path.exists(cand_path)
                and os.path.samefile(str(self.source_file), cand_path)
            )
        except OSError:
            return (
                os.path.normpath(str(self.source_file))
                == os.path.normpath(cand_path)
            )

    def _bytes_match(self, cand_path):
        if self._is_source(cand_path):
            return False
        sh = self._src_hash()
        if sh is None:
            return False
        try:
            return self.compute_file_hash(cand_path) == sh
        except OSError:
            return False


class _DuplicateCheckStream:
    """One ``/api/import/check-duplicates`` SSE stream.

    ``api_import_check_duplicates`` validates the body, then streams
    ``generate()``: index the catalog, prepare metadata in bounded chunks,
    check each path against the duplicate gate and the destination-recovery
    walk, and finish with the totals. The ``import_dedup``, ``ingest`` and
    ``scanner`` callables are the ones the view imported at request time.
    """

    def __init__(
        self,
        db_path,
        paths,
        *,
        verify_by_hash,
        include_capture_dates,
        skip_duplicates,
        recovery_base,
        folder_template,
        catalog_index,
        duplicate_checker,
        recover_companion_identities,
        source_capture_timestamps,
        build_destination_path,
        compute_file_hash,
    ):
        self.db_path = db_path
        self.paths = paths
        self.verify_by_hash = verify_by_hash
        self.include_capture_dates = include_capture_dates
        self.skip_duplicates = skip_duplicates
        self.recovery_base = recovery_base
        self.folder_template = folder_template
        self.catalog_index = catalog_index
        self.duplicate_checker = duplicate_checker
        self.recover_companion_identities = recover_companion_identities
        self.source_capture_timestamps = source_capture_timestamps
        self.build_destination_path = build_destination_path
        self.compute_file_hash = compute_file_hash

        # The checker still runs when skip_duplicates=False, but only for
        # its EXIF-batching side effect (needed by _recovery_candidate in
        # default mode). check_and_record() is skipped in the generator
        # below so no library-dedup verdict is produced — matching the
        # import job, which doesn't create the checker at all in that
        # mode. It is built inside the stream (see generate()), because
        # indexing the catalog can first have to hash paired JPEGs on a
        # network share.
        self.checker = None

        # str(path) -> capture datetime for recovery planning and day summaries when
        # verify_by_hash disables the checker's own EXIF batching.
        self.recovery_times = {}

        # name -> size per planned destination folder, one scandir each —
        # the destination may be a network mount, so listings are batched
        # rather than stat'ing per candidate file (count round trips).
        self.dir_listings = {}

        self.duplicate_count = 0
        self.recovered_count = 0

    def generate(self):
        yield from self._index_catalog()
        total = len(self.paths)
        yield from self._prepare_metadata(total)
        yield from self._check_paths(total)
        yield f"data: {json.dumps({'done': True, 'duplicate_count': self.duplicate_count, 'recovered_count': self.recovered_count, 'checked': total, 'total': total})}\n\n"

    def _index_catalog(self):
        """Index the catalog before any per-file work. Companion
        identities the catalog is missing are recovered here, a batch per
        frame, instead of before the response starts: a catalog with a
        thousand paired JPEGs on a NAS took minutes to hash, during which
        the page showed nothing, a superseded preview could not be stopped
        (no yield, so no disconnect), and every re-run started the whole
        hash over. The request's own DB is closed once the view returns,
        so the stream opens its own -- without re-running the schema pass,
        which app startup already did (the request connection skips it the
        same way)."""
        with Database(
            self.db_path, initialize_schema=(self.db_path == ":memory:"),
        ) as index_db:
            for checked, missing in self.recover_companion_identities(index_db):
                yield f"data: {json.dumps({'catalog_recovery': {'checked': checked, 'total': missing}})}\n\n"
            self.checker = self.duplicate_checker(
                self.catalog_index.from_db(index_db, recover_companions=False),
                verify_by_hash=self.verify_by_hash,
            )

    def _prepare_metadata(self, total):
        """Batch the EXIF header reads up front in bounded chunks (no-op
        in verify_by_hash mode). Intra-run duplicate tracking lives in the
        checker: identical source files not yet in the DB are reported as
        duplicates of each other, matching the actual import step.
        Chunking with a yield between each chunk lets a superseded browser
        request stop this phase within one chunk's worth of I/O — a single
        upfront prepare() over tens of thousands of files would otherwise
        ignore the disconnect entirely until the per-file loop begins."""
        prep_paths = [Path(p) for p in self.paths]
        prep_batch = DUPLICATE_CHECK_PREP_BATCH_SIZE
        for prep_start in range(0, len(prep_paths), prep_batch):
            chunk = prep_paths[prep_start:prep_start + prep_batch]
            self.checker.prepare(chunk)
            if (
                (self.recovery_base or self.include_capture_dates)
                and self.verify_by_hash
            ):
                # prepare() skipped the EXIF batch (verify mode's
                # identity is the hash), but recovery planning or day
                # summaries need capture times — resolve them alongside
                # the same chunk so both prep paths share the same
                # cancellation cadence.
                self.recovery_times.update({
                    str(f): dt
                    for f, dt in self.source_capture_timestamps(chunk).items()
                })
            # Cheap heartbeat frame the client can render as
            # "preparing metadata…" and, more importantly, the yield
            # that lets the WSGI server notice a disconnected client
            # between chunks instead of after the entire prep phase.
            prepared = prep_start + len(chunk)
            frame = {"preparing": prepared, "total": total}
            if self.include_capture_dates:
                # Share the metadata reads used for duplicate identity
                # and recovery planning with the day summary. The
                # discovery walk need not read these headers separately.
                dates = {}
                for source_file in chunk:
                    timestamp = self._capture_time(source_file)
                    dates[str(source_file)] = timestamp.date().isoformat() if timestamp else None
                frame["capture_dates"] = dates
            yield f"data: {json.dumps(frame)}\n\n"

    def _capture_time(self, source_file):
        if self.verify_by_hash:
            return self.recovery_times.get(str(source_file))
        return self.checker.capture_time(source_file)

    def _check_paths(self, total):
        batch_duplicates = []
        batch_recovered = []
        last_flush = time.monotonic()
        for checked, path in enumerate(self.paths, 1):
            # Zero-byte placeholders are non-duplicates (the checker
            # gives them no identity), and unreadable/missing files
            # are skipped; both fall through so the batch-yield block
            # below still runs. A `continue` would swallow any
            # already-queued `batch_duplicates` whenever such a file
            # landed on the last path or on a batch boundary, leaving
            # the UI unable to deselect those known dupes.
            try:
                # When skip_duplicates=False, the import run doesn't
                # consult the library-dedup checker at all — every
                # source file goes on to the recovery/adopt gate. Skip
                # check_and_record() here so a cataloged twin that
                # also sits at the destination is streamed as
                # ``recovered`` (matching what the run will actually
                # do) instead of ``duplicates`` (which the client
                # would then not subtract from the transfer count).
                if self.skip_duplicates and self.checker.check_and_record(
                        Path(path)):
                    batch_duplicates.append(path)
                    self.duplicate_count += 1
                elif self._recovery_candidate(path):
                    # Duplicate gate first, recovery second — same
                    # order as the import run, so a cataloged twin
                    # that also sits at the destination stays a
                    # duplicate here and a skip there.
                    batch_recovered.append(path)
                    self.recovered_count += 1
            except OSError:
                pass  # Skip unreadable/missing files

            # The yield is both how the client learns progress and how
            # the WSGI server notices that a superseded browser request
            # disconnected — cheap checks may finish dozens of files
            # inside one window (a single event covers them all), while
            # a slow byte-for-byte hash spends longer than the window on
            # one file (that file gets its own event and cancellation
            # stops within the next check). ``checked == total``
            # guarantees the last progress event always ships so the
            # client sees ``checked == total`` before ``done``.
            now = time.monotonic()
            if (
                checked == total
                or now - last_flush
                >= DUPLICATE_CHECK_FLUSH_INTERVAL_SECONDS
            ):
                yield f"data: {json.dumps({'duplicates': batch_duplicates, 'recovered': batch_recovered, 'checked': checked, 'total': total})}\n\n"
                batch_duplicates = []
                batch_recovered = []
                last_flush = now

    def _planned_folder_listing(self, folder):
        if folder not in self.dir_listings:
            entries = {}
            try:
                with os.scandir(folder) as it:
                    for entry in it:
                        try:
                            # Symlinks are deliberately EXCLUDED, and
                            # this is a considered trade, not an
                            # oversight. The import walk follows them
                            # (os.stat) and adopts a symlink to an
                            # off-card regular file whose bytes match,
                            # so excluding them makes the preview
                            # UNDER-report recovery for that geometry
                            # — it says "will copy" for a file the run
                            # adopts. That is the safe direction to be
                            # wrong in.
                            #
                            # Following them was tried (PR 7b) and
                            # reverted: the walk refuses a candidate
                            # resolving under ANY source root, while
                            # this endpoint's ``_is_source`` can only
                            # compare ``samefile`` against the CURRENT
                            # source file. A symlink to a *different*
                            # card file with identical bytes therefore
                            # slipped through and was reported as
                            # recovered — an OVER-claim, promising
                            # "already safe at the destination" for
                            # bytes that live only on the card. This
                            # endpoint receives ``paths``, not the
                            # import's source roots, so it cannot
                            # reconstruct that guard; doing this right
                            # needs the shared walk, i.e. the PR 8
                            # de-mirror. Trading a safe under-report
                            # for an unsafe over-report is not worth
                            # it in the meantime.
                            # Codex review of PR #1450, rounds 4-5.
                            if entry.is_file(follow_symlinks=False):
                                entries[entry.name] = entry.stat(
                                    follow_symlinks=False).st_size
                        except OSError:
                            continue
            except OSError:
                pass  # missing/unreadable folder -> nothing to adopt
            self.dir_listings[folder] = entries
        return self.dir_listings[folder]

    def _planned_folder(self, source_file):
        """Folder planning mirrors ingest._source_file_timestamps: EXIF
        capture time falling back to file mtime. In the default mode
        checker.prepare() already batched the EXIF reads and
        capture_time() is a cache hit; in verify mode prepare() is a
        no-op, so the times come from this request's own batch
        (``_prepare_metadata``) — never resolved lazily one file at a
        time. None when the template cannot render a folder."""
        ts = self._capture_time(source_file)
        if ts is None:
            with contextlib.suppress(OSError, ValueError,
                                     OverflowError):
                ts = datetime.fromtimestamp(
                    source_file.stat().st_mtime)
        try:
            rel_folder = self.build_destination_path(
                ts, self.folder_template, source_file)
        except ValueError:
            return None
        return (
            self.recovery_base if rel_folder in ("", ".")
            else os.path.join(self.recovery_base, rel_folder)
        )

    def _recovery_candidate(self, path):
        """True when the planned destination already holds a byte-
        identical file at the primary name OR at any suffix slot the
        run would adopt — mirrors ``import_job``'s adopt precondition
        (size match then byte-verify). A size-matching candidate whose
        bytes disagree advances the walk the same way a hash mismatch
        does in the run: otherwise the preview would subtract the
        file from "to copy" and promise "not re-copied" for a file
        the run will suffix-copy under a numbered name."""
        if not self.recovery_base:
            return False
        source_file = Path(path)
        try:
            size = source_file.stat().st_size
        except OSError:
            return False
        # NOTE: zero-byte sources are NOT special-cased here. They
        # used to return False on the reasoning that "the duplicate
        # checker gives them no identity either" — but that conflates
        # duplicate identity with crash-recovery adoption, which is
        # what this preview is about. ``_resolve_dest_collision``
        # adopts an empty candidate for an empty source at every
        # candidate position on both transports (spec PR 7b flip A;
        # the local primary-name case predates it), so returning
        # False here left the preview counting those files as
        # transfers the run would never perform. The generic path
        # below gets this right on its own: ``_src_hash`` uses
        # ``compute_file_hash``, so an empty source hashes to
        # EMPTY_FILE_SHA256 rather than the checker's None, and it
        # matches an empty candidate. Non-regular entries (FIFOs,
        # device nodes) stay excluded because
        # ``_planned_folder_listing`` only records
        # ``is_file(follow_symlinks=False)`` entries — which is also
        # what the run's own S_ISREG guard does. Codex review of
        # PR #1450.
        folder = self._planned_folder(source_file)
        if folder is None:
            return False
        listing = self._planned_folder_listing(folder)
        primary_name = source_file.name

        # Lazy source-hash: only computed once, and only if we hit a
        # size-matching candidate that needs verifying. A typical
        # fresh import has no size collisions and skips hashing
        # entirely.
        source = _RecoverySource(source_file, self.compute_file_hash)

        primary_size = listing.get(primary_name)
        primary_path = os.path.join(folder, primary_name)
        if primary_size == size and source._is_source(primary_path):
            # Destination candidate at the primary slot IS the
            # source file. The run fails this file entirely rather
            # than walking suffixes; report as not recovered instead
            # of falling through to the suffix walk (which could
            # find a coincidental byte-identical sibling in the
            # source folder and wrongly claim adoption).
            return False
        if primary_size == size:
            if source._bytes_match(primary_path):
                return True
            # Same size, different bytes at the primary slot: the run
            # will hash-mismatch and advance to the suffix walk.
        elif primary_size is None:
            # No collision on the primary name — the run copies to the
            # primary slot without walking suffixes.
            return False
        return _suffix_slot_recovered(
            source, folder, listing, primary_name, size)


def _suffix_slot_recovered(source, folder, listing, primary_name, size):
    """Primary slot is taken by a different-sized (or same-sized-
    different-bytes) file. Mirror import_job's collision walk
    (``name_1.ext``, ``name_2.ext``, ...): stop at the first free slot
    (the run would land a fresh copy there — not recovered), or claim
    recovery at the first byte-identical candidate (the run would adopt
    it). Size-mismatched slots advance the counter; same-size-different-
    bytes slots also advance, mirroring the run's hash-mismatch skip."""
    stem, suffix_ext = os.path.splitext(primary_name)
    counter = 1
    while True:
        candidate = f"{stem}_{counter}{suffix_ext}"
        cand_size = listing.get(candidate)
        if cand_size is None:
            return False
        if cand_size == size and source._bytes_match(
                os.path.join(folder, candidate)):
            return True
        counter += 1


@dataclass
class _ImportFullRequest:
    """The ``/api/jobs/import-full`` request body, read in the order the
    view always read it."""

    source: object
    destination: object
    file_types: object
    folder_template: object
    skip_duplicates: object
    verify_by_hash: bool
    copy: object
    exclude_paths: set

    @classmethod
    def from_body(cls, body):
        return cls(
            source=body.get("source", ""),
            destination=body.get("destination", ""),
            file_types=body.get("file_types", "both"),
            folder_template=body.get("folder_template", "%Y/%Y-%m-%d"),
            skip_duplicates=body.get("skip_duplicates", True),
            verify_by_hash=bool(body.get("verify_by_hash")),
            copy=body.get("copy", True),
            exclude_paths=set(body.get("exclude_paths", [])),
        )

    @property
    def scan_path(self):
        return self.destination if self.copy else self.source

    def validation_error(self):
        """The first 400 message for this request, or None when it is
        valid."""
        source = self.source
        destination = self.destination
        if not source:
            return "source is required"
        from image_loader import is_excluded_scan_path
        # See api_job_scan for why this must run before os.path.isdir.
        if is_excluded_scan_path(source):
            return (
                f"source is inside a macOS app-managed library and cannot "
                f"be imported: {source}"
            )
        if not os.path.isdir(source):
            return f"source directory not found: {source}"
        if self.copy:
            if not destination:
                return "source and destination are required"
            if not os.path.isabs(destination):
                return "destination must be an absolute path"
            from ingest import _is_unsafe_path
            if self.folder_template and _is_unsafe_path(self.folder_template):
                return "folder_template must be a relative path without '..' or backslashes"
        return None


def _import_scan_conflict(runner, db, scan_paths, workspace_id):
    """The local-copy conflict for an import scanning ``scan_paths``, or
    None. Callers hold ``stage_boundary_lock``."""
    pending_sources = stage_pending_source_paths(
        runner.list_jobs if runner is not None else None,
        db,
    )
    return local_copy_scan_conflict(
        db, scan_paths,
        active_workspace_id=workspace_id,
        pending_stage_sources=pending_sources,
    )


def _ingested_restrict_dirs(destination, copied_paths, duplicate_folders):
    """Build restrict_dirs from the folders ingest actually touched
    so the post-ingest scan doesn't re-walk the entire
    destination tree. Without this, importing ~2k RAWs into a
    populated library caused scanner.scan to enumerate tens of
    thousands of already-indexed files (observed: 59k). Mirrors
    the same pattern in pipeline_job.py. Only paths under the
    normalized destination are included; ".." tricks cannot
    escape. If nothing was copied and no duplicate folders were
    reported, restrict_dirs stays an empty list — scanner.scan
    then has no directories to enumerate, which matches intent
    (there is nothing new to index)."""
    dest_normalized = Path(os.path.normpath(destination))

    def _under_destination(path_str):
        try:
            return Path(os.path.normpath(path_str)).is_relative_to(
                dest_normalized
            )
        except ValueError:
            return False

    restrict_set = set()
    for cp in copied_paths:
        parent = str(Path(cp).parent)
        if _under_destination(parent):
            restrict_set.add(parent)
    for folder in duplicate_folders:
        if _under_destination(folder):
            restrict_set.add(folder)
    return sorted(restrict_set)


class _ImportFullRun:
    """One ``import-full`` job.

    ``api_job_import_full`` validates the request and its ``work`` closure
    runs one of these: copy files (when ``copy``), scan them into the
    catalog, generate thumbnails, and collect the imported photos into a
    new collection. ``config`` is the Flask app's config mapping, read at
    job time.
    """

    def __init__(self, ctx, params, config, invalidate_missing_originals, job):
        self.ctx = ctx
        self.params = params
        self.config = config
        self.invalidate_missing_originals = invalidate_missing_originals
        self.job = job
        self.thread_db = None
        self.scan_target = None
        self.restrict_dirs = None
        self.ingest_result = None
        self.copied_paths = None
        self.vireo_dir = None

    def run(self):
        from scanner import scan as do_scan
        from thumbnails import generate_all

        job = self.job
        self.thread_db = self.ctx.thread_db()
        # Check folder health before scanning to prevent duplicate imports
        if self.thread_db.check_folder_health():
            self.invalidate_missing_originals()
        job["_start_time"] = time.time()

        self.scan_target = str(Path(self.params.source))  # normalize (strips trailing slash)
        # restrict_dirs narrows the post-ingest scan to just the subfolders
        # that received files, instead of walking the full destination
        # tree. Populated in the copy branch from ingest_result's
        # copied_paths (parent dirs) and duplicate_folders. Left as None
        # for copy=false so scan-in-place keeps its original full-tree
        # behavior.
        self.restrict_dirs = None

        self._set_steps()
        if self.params.copy:
            self._ingest()
        self._scan(do_scan)
        self._generate_thumbnails(generate_all)
        return self._create_collection()

    def _set_steps(self):
        # Define steps based on whether we're copying
        steps = []
        if self.params.copy:
            steps.append({"id": "ingest", "label": "Import photos"})
        steps.extend([
            {"id": "scan", "label": "Scan photos"},
            {"id": "thumbnails", "label": "Generate thumbnails"},
            {"id": "collection", "label": "Create collection"},
        ])
        self.ctx.runner.set_steps(self.job["id"], steps)

    def _ingest(self):
        """Phase 1: Copy files."""
        from ingest import ingest as do_ingest

        ctx, job, params = self.ctx, self.job, self.params
        ctx.runner.update_step(job["id"], "ingest", status="running")

        def ingest_cb(current, total, filename):
            job["progress"]["current"] = current
            job["progress"]["total"] = total
            job["progress"]["current_file"] = filename
            ctx.runner.push_event(job["id"], "progress", {
                "current": current, "total": total,
                "current_file": filename,
                "phase": "Importing photos",
            })

        ingest_result = do_ingest(
            source_dir=params.source,
            destination_dir=params.destination,
            db=self.thread_db,
            file_types=params.file_types,
            folder_template=params.folder_template,
            skip_duplicates=params.skip_duplicates,
            verify_by_hash=params.verify_by_hash,
            progress_callback=ingest_cb,
            pause_callback=lambda: ctx.checkpoint(job),
            skip_paths=params.exclude_paths or None,
        )
        self.ingest_result = ingest_result
        self.copied_paths = ingest_result.get("copied_paths", [])
        duplicate_folders = ingest_result.get("duplicate_folders", [])
        self.scan_target = params.destination

        self.restrict_dirs = _ingested_restrict_dirs(
            params.destination, self.copied_paths, duplicate_folders,
        )

        ctx.runner.update_step(job["id"], "ingest", status="completed",
                           summary=f"{ingest_result.get('copied', 0)} copied")

    def _scan(self, do_scan):
        """Phase 2: Scan to index into DB."""
        ctx, job, thread_db = self.ctx, self.job, self.thread_db
        scan_target = self.scan_target
        ctx.checkpoint(job)
        ctx.runner.update_step(job["id"], "scan", status="running")

        def scan_cb(current, total):
            job["progress"]["current"] = current
            job["progress"]["total"] = total
            ctx.runner.push_event(job["id"], "progress", {
                "current": current, "total": total,
                "current_file": "",
                "phase": "Scanning photos",
            })

        # ``import-photos`` is a pausable job (registered as a
        # pause participant by the runner). Without these probes
        # the scan phase would keep hashing on its process pool
        # and hold its CPU lease across the entire pause, ignoring
        # the pause signal until the current source finishes.
        # Same wiring the in-place import path picked up in
        # 37e0e3a0 for the identical reason.
        #
        # ``ctx.runner.is_cancelled`` internally parks on Pause via
        # ``wait_if_paused``. Wrap the parking call in
        # ``suspend_resource_wait_timing`` so an hour-long pause
        # while a scan is waiting for CPU permits does not persist
        # as an hour of "resource contention" on the job's
        # diagnostics. The context manager is a no-op when no
        # ledger wait is active, so it's safe on non-pausable
        # invocations too. Mirrors what ``_pause_checkpoint``
        # does for pipeline participants (pipeline_job.py:1567).
        def scan_cancel_check():
            from resource_ledger import suspend_resource_wait_timing
            with suspend_resource_wait_timing():
                return ctx.runner.is_cancelled(job["id"])

        def scan_pause_check():
            return ctx.runner.pause_requested(job["id"])

        def scan_cancel_only_check():
            return ctx.runner.cancellation_requested(job["id"])

        self.vireo_dir = os.path.dirname(self.config["THUMB_CACHE_DIR"])
        try:
            # copy=false: scan_target is the source and restrict_dirs is
            #   None, so scanner walks the full source tree (unchanged).
            # copy=true: scan_target is the destination (folder hierarchy
            #   root, for parent-folder chain creation), but restrict_dirs
            #   narrows enumeration to only the subfolders ingest wrote
            #   into. An empty list means "nothing new to scan" — a no-op
            #   inside scanner.scan.
            do_scan(
                scan_target, thread_db,
                progress_callback=scan_cb,
                skip_paths=self.params.exclude_paths or None,
                vireo_dir=self.vireo_dir,
                thumb_cache_dir=self.config["THUMB_CACHE_DIR"],
                restrict_dirs=self.restrict_dirs,
                cancel_check=scan_cancel_check,
                pause_check=scan_pause_check,
                cancel_only_check=scan_cancel_only_check,
            )
        finally:
            # scanner.scan commits photo rows incrementally, so even a mid-scan
            # failure can leave DB state that invalidates cached new-image counts.
            invalidate_new_images_after_scan(thread_db, scan_target)
            # scanner.scan touches disk and may reconcile ghost rows
            # (e.g. a user restored an original before running import).
            # The pre-scan health-check invalidation only fires when a
            # folder flips missing/ok, so also drop the missing-originals
            # cache once the scan itself has run — even on partial
            # failure, since rows are committed incrementally.
            try:
                self.invalidate_missing_originals()
            except Exception:
                log.exception(
                    "Failed to invalidate missing-originals cache after import scan of %s",
                    scan_target,
                )
        scan_count = job["progress"].get("total", 0)
        scan_summary = f"{scan_count} photos"
        metadata_warning = scan_metadata_warning()
        if metadata_warning:
            scan_summary += f" — {metadata_warning}"
        ctx.runner.update_step(job["id"], "scan", status="completed",
                           summary=scan_summary)

    def _generate_thumbnails(self, generate_all):
        """Phase 3: Generate thumbnails."""
        ctx, job = self.ctx, self.job
        ctx.checkpoint(job)
        ctx.runner.update_step(job["id"], "thumbnails", status="running")
        ctx.runner.push_event(job["id"], "progress", {
            "current": 0, "total": 0,
            "current_file": "Checking for new thumbnails...",
            "phase": "Generating thumbnails",
        })

        def thumb_cb(current, total):
            job["progress"]["current"] = current
            job["progress"]["total"] = total
            ctx.runner.push_event(job["id"], "progress", {
                "current": current, "total": total,
                "current_file": "",
                "phase": "Generating thumbnails",
            })

        thumb_result = generate_all(
            self.thread_db, self.config["THUMB_CACHE_DIR"],
            progress_callback=thumb_cb,
            cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
            vireo_dir=self.vireo_dir,
        )
        from thumbnails import format_summary as thumb_summary
        ctx.runner.update_step(job["id"], "thumbnails", status="completed",
                           summary=thumb_summary(thumb_result))

    def _imported_photo_ids(self):
        thread_db = self.thread_db
        photo_ids = []
        if self.params.copy:
            # Collection from copied files (existing logic)
            copied_paths = self.copied_paths
            if copied_paths:
                thread_db.conn.execute(
                    "CREATE TEMP TABLE IF NOT EXISTS _imported_paths (dirpath TEXT, fname TEXT)"
                )
                thread_db.conn.execute("DELETE FROM _imported_paths")
                thread_db.conn.executemany(
                    "INSERT INTO _imported_paths (dirpath, fname) VALUES (?, ?)",
                    [(os.path.dirname(p), os.path.basename(p)) for p in copied_paths],
                )
                rows = thread_db.conn.execute(
                    """SELECT p.id FROM photos p
                       JOIN folders f ON p.folder_id = f.id
                       JOIN _imported_paths ip ON f.path = ip.dirpath
                                               AND p.filename = ip.fname"""
                ).fetchall()
                photo_ids = [r["id"] for r in rows]
                thread_db.conn.execute("DROP TABLE IF EXISTS _imported_paths")
        else:
            # Collection from all photos in the scanned folder
            scan_target = self.scan_target
            rows = thread_db.conn.execute(
                """SELECT p.id FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   WHERE f.path = ? OR f.path LIKE ?""",
                (scan_target, scan_target.rstrip("/") + "/%"),
            ).fetchall()
            photo_ids = [r["id"] for r in rows]
        return photo_ids

    def _create_collection(self):
        """Phase 4: Create collection."""
        ctx, job = self.ctx, self.job
        ctx.checkpoint(job)
        ctx.runner.update_step(job["id"], "collection", status="running")
        photo_ids = self._imported_photo_ids()

        collection_id = None
        collection_name = None
        if photo_ids:
            from datetime import datetime as dt
            collection_name = "Import " + dt.now().strftime("%Y-%m-%d %H:%M")
            collection_id = self.thread_db.add_collection(
                collection_name,
                json.dumps([{"field": "photo_ids", "value": photo_ids}]),
            )

        col_summary = collection_name if collection_name else "no photos"
        ctx.runner.update_step(job["id"], "collection", status="completed",
                           summary=col_summary)

        result = {
            "photos_indexed": len(photo_ids),
            "collection_id": collection_id,
            "collection_name": collection_name,
        }
        if self.params.copy:
            ingest_result = self.ingest_result
            result["copied"] = ingest_result.get("copied", 0)
            result["skipped_duplicate"] = ingest_result.get("skipped_duplicate", 0)
            result["failed"] = ingest_result.get("failed", 0)
            result["total"] = ingest_result.get("total", 0)

        return result


def create_imports_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    invalidate_missing_originals,
    enqueue_process_job,
    chain_after_move,
    bulk_gps_location_payload,
    guard_move_folder,
    sync_job_lock,
):
    """Build the imports blueprint.

    ``config`` is the Flask app's config mapping (read at request and job
    time for ``THUMB_CACHE_DIR`` and ``REQUIRE_EXIFTOOL_FOR_IMPORT``, so
    runtime overrides keep working). The keyword arguments are per-app
    service methods; see the module docstring.
    """
    blueprint = Blueprint("imports", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)
    archive_dispatch_lock = threading.Lock()
    imports = ImportService(
        get_runner, db_path, config,
        invalidate_missing_originals=invalidate_missing_originals,
        enqueue_process_job=enqueue_process_job,
        chain_after_move=chain_after_move,
        bulk_gps_location_payload=bulk_gps_location_payload,
    )

    def import_response(result):
        if isinstance(result, ImportFailure):
            if result.details is not None:
                return jsonify({"error": result.message, **result.details}), result.status
            return json_error(result.message, result.status)
        return jsonify(result)

    @blueprint.post("/api/jobs/import-in-place")
    def api_job_import_in_place():
        body = request.get_json(silent=True) or {}
        return import_response(imports.import_in_place(get_db(), body))

    @blueprint.post("/api/jobs/import-photos")
    def api_job_import_photos():
        body = request.get_json(silent=True) or {}
        return import_response(imports.import_photos(get_db(), body))

    @blueprint.get("/api/import/pending-archives")
    def api_pending_archives():
        from pending_archives import active_archive_jobs
        db = get_db()
        jobs = active_archive_jobs(get_runner(), db._ws_id())
        rows = db.conn.execute(
            "SELECT a.*, c.id AS review_collection_id, c.name AS collection_name FROM pending_archives a "
            "LEFT JOIN collections c ON c.id = a.collection_id AND c.workspace_id = a.workspace_id "
            "WHERE a.workspace_id = ? AND a.state != 'complete' ORDER BY a.created_at",
            (db._ws_id(),),
        ).fetchall()
        # Only pay for the folder read when something is actually pending —
        # three pages poll this endpoint every 5s with an empty list most of
        # the time.
        ws_folders = _all_folders(db) if rows else []
        items = []
        for row in rows:
            sending = any(j.get("type") == "send-to-nas"
                          and (j.get("config") or {}).get("pending_archive_id") == row["id"] for j in jobs)
            folder_ids = _folder_ids_under(ws_folders, row["staging_destination"])
            _changes, here, elsewhere, overlap = db.staged_sync_scope(folder_ids)
            items.append({
                "id": row["id"], "destination": row["destination"],
                "folder_ids": folder_ids,
                # Scoped to this archive's staging tree, not the catalog-wide
                # sync queue: the banner offers to sync *these* photos, so the
                # number has to be the ones the transfer would leave stale.
                # ``_other_workspaces`` is what this sync will NOT write, kept
                # separate so neither number over-promises.
                # ``_here_with_sibling_edits`` is the overlap: photos already
                # counted in ``unsynced_photos`` that also have edits queued in
                # a sibling workspace. Left out of ``other_workspaces`` because
                # it would read as extra photos rather than the same photo
                # carrying two workspaces' edits, and surfaced on its own so
                # the banner can warn that the sibling's changes on those
                # photos will not be written by this button.
                "unsynced_photos": here,
                "unsynced_photos_other_workspaces": elsewhere,
                "unsynced_photos_here_with_sibling_edits": overlap,
                "source_available": os.path.isdir(row["staging_destination"]),
                "collection_id": row["review_collection_id"], "name": row["collection_name"] or "Imported photos",
                "state": "sending" if sending else "waiting" if jobs else "ready",
                "error": row["error"] or (
                    "The previous transfer was interrupted. Local originals are retained; try sending again."
                    if row["state"] == "sending" and not sending else ""),
            })
        return jsonify({"items": items})

    @blueprint.post("/api/import/pending-archives/<archive_id>/discard")
    def api_discard_pending_archive(archive_id):
        from pending_archives import active_archive_jobs, get_pending_archive
        db = get_db()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or body.get("confirmed") is not True:
            return json_error("Confirm removal of this missing NAS transfer record", 400)
        with archive_dispatch_lock:
            archive = get_pending_archive(db, archive_id)
            if archive is None:
                return json_error("Pending NAS transfer not found in this workspace", 404)
            blocking = active_archive_jobs(get_runner(), db._ws_id())
            if blocking:
                return json_error(
                    f"Wait for {describe_jobs(blocking)} to finish before removing this transfer record", 409)
            if os.path.isdir(archive["staging_destination"]):
                return json_error("Local originals are available. Send them to NAS before removing this transfer", 409)
            # Forget only the transfer, never files or catalog entries. This is
            # explicit recovery for lost storage, including interrupted sends.
            db.conn.execute(
                "DELETE FROM pending_archives WHERE id = ? AND workspace_id = ?",
                (archive_id, db._ws_id()),
            )
            db.conn.commit()
        return jsonify({"ok": True})

    @blueprint.post("/api/import/pending-archives/<archive_id>/send")
    def api_send_pending_archive(archive_id):
        from pending_archives import active_archive_jobs, get_pending_archive, send_pending_archive
        db = get_db()
        runner = get_runner()
        workspace_id = db._ws_id()
        raw = request.get_data(cache=True, as_text=True)
        # ``get_json(silent=True)`` returns None for both an absent body and a
        # parse failure, and ``or {}`` would additionally swallow the falsy
        # literals false / 0 / [] / "". Any of those would start the one-way
        # transfer as a plain send for a request that never asked for one.
        if raw and raw.strip():
            try:
                body = json.loads(raw)
            except ValueError:
                return json_error("Request body must be valid JSON")
            if not isinstance(body, dict):
                return json_error("Request body must be a JSON object")
        else:
            body = {}
        sync_first = body.get("sync_first", False)
        # Not coerced: bool("false") is True, and this option writes sidecars
        # and can fail a transfer.
        if not isinstance(sync_first, bool):
            return json_error("sync_first must be a boolean")
        with archive_dispatch_lock:
            archive = get_pending_archive(db, archive_id)
            if archive is None:
                return json_error("Pending NAS transfer not found in this workspace", 404)
            if archive["state"] == "complete":
                return jsonify({"already_sent": True})
            active = active_archive_jobs(runner, workspace_id)
            existing = next((j for j in active if j.get("type") == "send-to-nas"
                             and (j.get("config") or {}).get("pending_archive_id") == archive_id), None)
            if existing:
                # Joining is only honest when the running job is doing what
                # this caller asked for. A plain transfer cannot be upgraded
                # mid-flight, so handing back its id would promise a metadata
                # sync that is never going to run.
                if bool((existing.get("config") or {}).get("sync_first")) != sync_first:
                    return json_error(
                        "These photos are already being sent to NAS "
                        + ("without the metadata sync" if sync_first
                           else "with the metadata sync")
                        + ". Wait for that transfer to finish before starting "
                        "a different one.",
                        409,
                    )
                return jsonify({"job_id": existing["id"]})
            if active:
                return json_error(
                    f"Wait for {describe_jobs(active)} to finish before sending these photos to NAS", 409)

            def work(job):
                with Database(db_path) as thread_db:
                    thread_db.set_active_workspace(workspace_id)
                    thread_db.conn.execute(
                        "UPDATE pending_archives SET state = 'sending', error = '' WHERE id = ?",
                        (archive_id,),
                    )
                    thread_db.conn.commit()
                    steps = None
                    try:
                        remote = json.loads(archive["target_json"]).get("transport") != "mounted"
                        steps = MoveSteps(
                            runner, job,
                            ([{"id": "sync", "label": "Write metadata to sidecars"}] if sync_first else [])
                            + folder_move_steps(remote=remote, verify_contents=not remote),
                            phases={**FOLDER_MOVE_PHASES,
                                    "Waiting for current XMP sync": "sync",
                                    "Writing metadata to sidecars": "sync"},
                        )
                        steps.start()

                        def progress(current, total, filename, phase="Sending to NAS"):
                            job["progress"].update(current=current, total=total, current_file=filename)
                            if not steps.report(current, total, filename, phase):
                                return
                            runner.push_event(job["id"], "progress", {
                                "current": current, "total": total, "current_file": filename, "phase": phase,
                                # The count belongs to this phase, not the whole
                                # transfer; label it so nothing reads it as "Overall".
                                "phase_current": current, "phase_total": total, "phase_label": phase,
                            })

                        folder_ids = _staging_folder_ids(
                            thread_db, archive["staging_destination"]) if sync_first else []
                        # Photo ids are captured before the move so the
                        # post-transfer residual check still finds late edits
                        # after a tracked-merge reparent onto the destination
                        # folder ids. See _residual_staged_changes.
                        staged_photo_ids = (
                            _staged_photo_ids(thread_db, folder_ids)
                            if sync_first else []
                        )
                        if sync_first and not sync_job_lock.acquire(blocking=False):
                            # Cancellably: the wait can be minutes if another
                            # workspace's XMP sync holds the lock, and nothing
                            # on disk has been touched yet. Mirrors the
                            # /api/jobs/sync route's own acquire.
                            progress(0, 0, "", "Waiting for current XMP sync")
                            while not sync_job_lock.acquire(timeout=0.1):
                                if runner.is_cancelled(job["id"]):
                                    raise ValueError(
                                        "Transfer cancelled before it started. "
                                        "Local originals are retained.")
                        try:
                            # Only now: the lock is in hand and the next step
                            # touches files.
                            if not runner.begin_uncancellable(job["id"]):
                                raise ValueError("Transfer cancelled before it started. Local originals are retained.")

                            # Before the move, while the sidecars are still on
                            # local disk. Recomputed here rather than trusting
                            # the count the banner showed, so edits queued
                            # between the click and the job travel too.
                            synced, undeliverable = _sync_staged_metadata(
                                thread_db, progress, sync_job_lock,
                                folder_ids) if sync_first else (0, set())

                            # Transfer preflight can fail before move_folder's
                            # first callback. The successful sync is already over.
                            progress(0, 0, "", "Checking destination")
                            result = send_pending_archive(
                                thread_db, archive, vireo_dir=os.path.dirname(config["THUMB_CACHE_DIR"]),
                                guard_folder=guard_move_folder, progress_cb=progress,
                            )
                            if sync_first:
                                # Unconditionally, not just when something
                                # synced: a queue holding only changes this
                                # workspace declines to write syncs nothing,
                                # and an edit made during the copy would then
                                # go unreported.
                                #
                                # ``preserved_off_staging_identities`` covers
                                # edits the tracked-merge path remapped onto
                                # a survivor NOT in ``staged_photo_ids`` --
                                # the archive-side row of a byte-identical
                                # real collision, whose id the by-photo
                                # residual re-read below cannot see. The
                                # phantom-target and intra-staged cases
                                # leave the survivor inside
                                # ``staged_photo_ids`` and the residual
                                # re-read finds them on its own; adding the
                                # full ``preserved_edit_count`` here would
                                # double-count those. See
                                # merge_staged_tree_into_archive.
                                #
                                # Reported as identities, not a raw
                                # rowcount: filtering ``undeliverable``
                                # here keeps rows the pre-transfer drain
                                # deliberately did not write (a flag under
                                # ``sync_flags_to_xmp`` off) out of the
                                # "queued during transfer" count -- they
                                # predate the transfer and were considered
                                # by the sync. Sibling-workspace edits are
                                # already dropped upstream by
                                # ``merge_staged_tree_into_archive``.
                                residual = _residual_staged_changes(
                                    thread_db, staged_photo_ids, undeliverable)
                                off_staging = result.get(
                                    "preserved_off_staging_identities", []) or []
                                off_staging_residual = sum(
                                    1 for ident in off_staging
                                    if ident not in undeliverable
                                )
                                if residual is not None:
                                    residual += off_staging_residual
                        finally:
                            if sync_first:
                                sync_job_lock.release()
                        if sync_first:
                            summary = result["summary"]
                            if synced:
                                summary = (
                                    f"Synced metadata for {synced} photo"
                                    f"{'' if synced == 1 else 's'}. {summary}"
                                )
                            if residual is None:
                                # The re-read failed, not the transfer. Saying
                                # nothing here would read as "nothing missed
                                # the transfer" -- the one claim this job
                                # cannot make.
                                summary += (
                                    ". Could not re-check the sync queue "
                                    "afterwards, so whether any edit was "
                                    "queued during the transfer is unknown"
                                )
                            elif residual:
                                summary += (
                                    f". {residual} edit{'' if residual == 1 else 's'} "
                                    "queued during the transfer and still need a "
                                    "sync, now over the NAS connection"
                                )
                            result = {
                                **result, "metadata_synced": synced,
                                "metadata_queued_during_transfer": residual,
                                "summary": summary,
                            }
                        # A stepped job's panel shows only its steps, so the
                        # outcomes that need the user's attention go on the
                        # step they came from.
                        steps.finish()
                        if sync_first:
                            steps.finish("sync", summary=(
                                f"{synced} photo{'' if synced == 1 else 's'}"
                            ), error=(
                                "Could not re-check the sync queue afterwards, so whether any "
                                "edit was queued during the transfer is unknown"
                                if residual is None else
                                f"{residual} edit{'' if residual == 1 else 's'} queued during the "
                                "transfer and still need a sync, now over the NAS connection"
                                if residual else None))
                        if result.get("cleanup_error"):
                            steps.finish("cleanup", error=(
                                f"Local cleanup needs attention at {archive['staging_destination']}: "
                                f"{result['cleanup_error']}"))
                        thread_db.conn.execute(
                            "UPDATE pending_archives SET state = 'complete', error = '' WHERE id = ?", (archive_id,),
                        )
                        thread_db.conn.commit()
                        try:
                            invalidate_missing_originals()
                        except Exception:
                            # The archive already completed; a stale Missing
                            # Originals cache refreshes on its next scan.
                            log.warning("Could not invalidate Missing Originals after archive", exc_info=True)
                        return result
                    except Exception as e:
                        if steps is not None:
                            steps.fail(str(e), status="cancelled" if runner.is_cancelled(job["id"]) else "failed")
                        thread_db.conn.execute(
                            "UPDATE pending_archives SET state = 'pending', error = ? WHERE id = ?",
                            (str(e), archive_id),
                        )
                        thread_db.conn.commit()
                        raise

            job_id, _, _ = runner.start_singleton(
                "send-to-nas", work, singleton_key=archive_id,
                workspace_id=workspace_id,
                exclusive_workspace=True,
                config={"pending_archive_id": archive_id, "destination": archive["destination"],
                        "sync_first": sync_first},
            )
        return jsonify({"job_id": job_id})
    @blueprint.route("/api/import/preview", methods=["POST"])
    def api_import_preview():
        db = get_db()
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        if not catalogs:
            return json_error("catalogs required")
        try:
            from importer import preview_import

            result = preview_import(catalogs, db)
            return jsonify(result)
        except Exception as e:
            log.exception("Catalog import preview failed")
            return json_error(str(e), 500)

    @blueprint.route("/api/import/folder-preview-stream", methods=["POST"])
    def api_import_folder_preview_stream():
        """One storage-aware traversal streams per-folder scan progress and
        ends with the preview payload — the source-row counters and the
        preview grid share a single walk instead of scanning twice."""
        body = request.get_json(silent=True) or {}
        folders = body.get("folders", [])
        if (
            not isinstance(folders, list)
            or not folders
            or any(not isinstance(path, str) or not path for path in folders)
        ):
            return json_error("folders must be a non-empty list of paths", 400)
        file_types = body.get("file_types", [])
        return Response(
            source_discovery.stream_folder_preview(
                folders,
                file_types=file_types if file_types else "both",
                recursive=bool(body.get("recursive", True)),
                include_capture_dates=bool(body.get("include_capture_dates", False)),
            ),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @blueprint.route("/api/import/new-images-preview", methods=["POST"])
    def api_import_new_images_preview():
        """Preview grid data for a new-images snapshot, matching the
        folder-preview response shape so the same client renderer works."""
        body = request.get_json(silent=True) or {}
        snapshot_id = body.get("snapshot_id")
        if not isinstance(snapshot_id, int):
            return json_error("snapshot_id required", 400)

        db = get_db()
        if db._active_workspace_id is None:
            abort(404)
        try:
            snap = db.get_new_images_snapshot(snapshot_id)
        except OverflowError:
            snap = None
        if snap is None:
            abort(404)

        # Use user-facing roots (not every auto-linked descendant) so
        # grouping matches the source folders. Include roots currently marked
        # missing: a snapshot preview must retain the folder and unavailable
        # file the banner promised instead of silently losing its provenance.
        from new_images import mapped_roots as _ni_mapped_roots

        root_paths = [
            r["path"]
            for r in _ni_mapped_roots(
                db, db._active_workspace_id, include_missing=True,
            )
        ]

        # Build unique display names across roots by taking the shortest
        # trailing path segments that are unique — so /mnt/cardA/DCIM and
        # /mnt/cardB/DCIM become cardA/DCIM and cardB/DCIM rather than
        # colliding on "DCIM". Mirrors folder-preview's disambiguation.
        root_names = {}
        if len(root_paths) > 1:
            parts = [Path(rp).parts for rp in root_paths]
            for depth in range(1, max(len(p) for p in parts) + 1):
                suffixes = [str(Path(*p[-depth:])) for p in parts]
                if len(set(suffixes)) == len(suffixes):
                    for rp, suffix in zip(root_paths, suffixes, strict=True):
                        root_names[rp] = suffix
                    break
            else:
                for rp in root_paths:
                    root_names[rp] = rp
        else:
            for rp in root_paths:
                root_names[rp] = os.path.basename(rp.rstrip("/")) or rp

        roots = sorted(
            [(rp, root_names[rp]) for rp in root_paths],
            key=lambda pn: len(pn[0]),
            reverse=True,
        )

        def _subfolder_for(path):
            for root_path, root_name in roots:
                try:
                    rel = Path(path).parent.relative_to(root_path)
                except ValueError:
                    continue
                rel_str = str(rel)
                return root_name if rel_str == "." else os.path.join(root_name, rel_str)
            return os.path.dirname(path) or "."

        files = []
        type_breakdown = {}
        total_size = 0
        unavailable_count = 0
        for path in snap["file_paths"]:
            try:
                stat = os.stat(path)
            except OSError:
                unavailable_count += 1
                ext = os.path.splitext(path)[1].lower()
                files.append({
                    "path": path,
                    "filename": os.path.basename(path),
                    "subfolder": _subfolder_for(path),
                    "size": 0,
                    "extension": ext,
                    "mtime": None,
                    "available": False,
                    "error": "File is no longer available",
                })
                type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
                continue
            ext = os.path.splitext(path)[1].lower()
            files.append({
                "path": path,
                "filename": os.path.basename(path),
                "subfolder": _subfolder_for(path),
                "size": stat.st_size,
                "extension": ext,
                "mtime": stat.st_mtime,
                "available": True,
                "thumb_url": "/api/import/folder-preview/thumbnail?path=" + quote(path),
            })
            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += stat.st_size

        return jsonify({
            "total_count": snap["file_count"],
            "available_count": len(files) - unavailable_count,
            "unavailable_count": unavailable_count,
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @blueprint.route("/api/import/check-duplicates", methods=["POST"])
    def api_import_check_duplicates():
        """Stream duplicate detection results via SSE.

        Accepts {"paths": [...], "verify_by_hash": bool} and streams
        batches of duplicate paths back to the client. Uses the same
        DuplicateChecker as ingest() — metadata-first with a content-hash
        fallback by default, hash-everything when verify_by_hash — so the
        preview's DUPLICATE badges and count are exactly the files the
        import step will skip.

        Optional {"destination": abs path, "folder_template": str} turns on
        destination-recovery detection: non-duplicate files whose planned
        destination folder already holds a byte-identical file at the
        primary name — or at a numeric-suffix slot the run would adopt
        (``name_1.ext``, ``name_2.ext``, ...) — are streamed as
        ``recovered`` (with a final ``recovered_count``). Those are the
        files a cancelled/crashed prior run left at the destination —
        the import adopts them via crash recovery (verify + catalog, no
        re-copy), so counting them "to copy" overstates the transfer.

        A size match is the cheap gate: same-size candidates are the only
        ones the run would even byte-verify, so hashing is scoped to that
        subset (rare in a fresh import; proportional to actual collisions
        in a retry). A same-size candidate whose bytes disagree advances
        the walk the same way ``import_job`` does on a hash mismatch —
        otherwise the preview would tell the user "not re-copied" for a
        file the run is about to suffix-copy under a numbered name.

        Optional {"skip_duplicates": false} matches the import job's own
        gate: when the run has duplicate skipping off, the library-dedup
        checker isn't consulted (library duplicates get copied anyway),
        but crash-recovery adoption of byte-identical files at the
        destination still fires. Passing skip_duplicates=false here
        mirrors that: no ``duplicates`` are streamed, but ``recovered``
        still is — so the retry preview after a cancelled dedup-off run
        doesn't overstate the transfer.

        With include_capture_dates=true, preparation frames also carry
        capture_dates (path to ISO date or null), sharing metadata reads
        with duplicate checking and recovery planning.
        """
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        verify_by_hash = bool(body.get("verify_by_hash"))
        include_capture_dates = bool(body.get("include_capture_dates", False))
        # Default True is the import job's default and preserves the
        # pre-existing endpoint contract; only the dedup-off preview
        # branch passes False.
        skip_duplicates = bool(body.get("skip_duplicates", True))
        if not paths:
            return json_error("paths required", 400)

        from import_dedup import (
            CatalogIndex,
            DuplicateChecker,
            recover_companion_identities,
            source_capture_timestamps,
        )
        from ingest import _is_unsafe_path, build_destination_path
        from scanner import compute_file_hash

        recovery_base = (body.get("destination") or "").strip()
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if recovery_base and not os.path.isabs(recovery_base):
            return json_error("destination must be an absolute path", 400)
        if recovery_base and folder_template and _is_unsafe_path(
                folder_template):
            return json_error(
                "folder_template must be a relative path without '..' "
                "or backslashes", 400)

        stream = _DuplicateCheckStream(
            db_path,
            paths,
            verify_by_hash=verify_by_hash,
            include_capture_dates=include_capture_dates,
            skip_duplicates=skip_duplicates,
            recovery_base=recovery_base,
            folder_template=folder_template,
            catalog_index=CatalogIndex,
            duplicate_checker=DuplicateChecker,
            recover_companion_identities=recover_companion_identities,
            source_capture_timestamps=source_capture_timestamps,
            build_destination_path=build_destination_path,
            compute_file_hash=compute_file_hash,
        )
        return Response(
            stream.generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @blueprint.route("/api/import/collection-preview", methods=["POST"])
    def api_import_collection_preview():
        """Return preview data for photos in a collection."""
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        if not collection_id:
            return json_error("collection_id required", 400)

        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        try:
            photos = db.get_collection_photos(collection_id, page=1, per_page=100000)
        except ValueError as e:
            log.exception(
                "Collection %s has unresolvable rules", collection_id
            )
            return json_error(f"collection rules cannot be resolved: {e}", 400)

        folder_rows = db.conn.execute("SELECT id, path, name FROM folders").fetchall()
        folder_map = {r["id"]: dict(r) for r in folder_rows}

        files = []
        type_breakdown = {}
        total_size = 0

        for p in photos:
            folder = folder_map.get(p["folder_id"], {})
            folder_name = folder.get("name", "Unknown")
            ext = (p["extension"] or "").lower()
            size = p["file_size"] or 0
            folder_path = folder.get("path", "")
            full_path = os.path.join(folder_path, p["filename"]) if folder_path else p["filename"]

            files.append({
                "path": full_path,
                "filename": p["filename"],
                "subfolder": folder_name,
                "size": size,
                "extension": ext,
                "mtime": p["file_mtime"] or 0,
                "thumb_url": f"/thumbnails/{p['id']}.jpg",
                "duplicate": False,
                "photo_id": p["id"],
            })

            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += size

        return jsonify({
            "total_count": len(files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @blueprint.route("/api/import/destination-preview", methods=["POST"])
    def api_import_destination_preview():
        """Preview destination folder structure without copying files."""
        body = request.get_json(silent=True) or {}
        sources = body.get("sources", [])
        destination = body.get("destination", "")
        if not sources:
            return json_error("sources required", 400)
        if not destination:
            return json_error("destination required", 400)
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path", 400)

        from ingest import _is_unsafe_path, preview_destination

        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if folder_template and _is_unsafe_path(folder_template):
            return json_error("folder_template must be a relative path without '..' or backslashes", 400)

        try:
            result = preview_destination(
                sources=sources,
                destination=destination,
                folder_template=folder_template,
                file_types=body.get("file_types", "both"),
                recursive=body.get("recursive", True),
                exclude_paths=body.get("exclude_paths"),
            )
        except ValueError as e:
            return json_error(str(e), 400)

        # Transparency: if the destination is (or sits inside) a folder Vireo
        # already manages, surface it as an existing archive so the UI can
        # frame the import as a merge rather than a fresh copy. Do NOT accept
        # the inverse relationship (a tracked folder somewhere below the
        # selected destination): selecting a broad mount such as
        # /Volumes/Photography while a managed archive lives at
        # /Volumes/Photography/Raw Files/USA does not mean a new import into
        # /Volumes/Photography/2026 will merge into that nested archive.
        # Calling the overlap helper here produced exactly that contradictory
        # preview. Pure catalog read — no file I/O beyond the tracked-folder
        # probe.
        from move import _tracked_destination_ancestor
        db = get_db()
        from db import _subtree_prefix

        def _archive_photo_count(archive_path):
            prefix = _subtree_prefix(archive_path)
            # Count only ok/partial folders — the same set ingest treats as
            # "the archive" — so the callout's "N photos" matches what the
            # merge actually considers present. Pure catalog read; no on-disk
            # check.
            return db.conn.execute(
                """SELECT COUNT(*) AS c
                     FROM photos p JOIN folders f ON f.id = p.folder_id
                    WHERE (f.path = ?
                           OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?)
                      AND f.status IN ('ok', 'partial')""",
                (archive_path, len(prefix), prefix),
            ).fetchone()["c"]

        managed_archives = []
        managed_archive = None

        dest_tracked = _tracked_destination_ancestor(db, -1, destination)
        if dest_tracked is not None:
            # The destination itself is (or sits inside) a tracked archive, so
            # every generated folder lands inside it — full coverage.
            managed_archive = {
                "path": dest_tracked["path"],
                "photo_count": _archive_photo_count(dest_tracked["path"]),
            }
            managed_archives = [dict(managed_archive, coverage="full")]
        else:
            # The destination itself may sit above every tracked archive while
            # the folder template still maps generated files into one — e.g.
            # destination /Photography with a tracked root /Photography/2026
            # and template 2026/%Y-%m-%d lands every file inside the managed
            # /Photography/2026 archive. Checking only the destination's
            # ancestors leaves managed_archive None even though the import IS
            # a merge into a tracked archive. Walk the concrete full_path
            # values from preview_destination() so we catch that case, while
            # still avoiding the sibling-folder false positive the
            # destination-only guard was written to prevent (a broad mount
            # whose tracked archive is a sibling of the generated folders
            # never becomes an ancestor of any full_path).
            #
            # Do NOT collapse mixed coverage to a single archive. A source
            # spanning multiple date-templated folders can split so that some
            # generated folders land inside a tracked archive and others land
            # outside it, or land in DIFFERENT tracked archives — for example
            # files from 2025 and 2026 with destination /Photography, template
            # %Y/%Y-%m-%d, and only /Photography/2026 tracked (2025 files land
            # outside the archive), or the same source but with both
            # /Photography/2025 and /Photography/2026 tracked (each subset
            # lands in a different archive). Breaking at the first match and
            # asserting "this import lands inside archive X" for the whole
            # preview lies about the other subsets. Aggregate the matches and
            # expose partial/multiple overlaps distinctly.
            folder_entries = result.get("folders") or []
            per_archive = {}  # archive_path -> matched_folder_count
            unmatched = 0
            considered = 0
            for entry in folder_entries:
                full_path = entry.get("full_path")
                if not full_path:
                    continue
                considered += 1
                candidate = _tracked_destination_ancestor(db, -1, full_path)
                if candidate is None:
                    unmatched += 1
                else:
                    key = candidate["path"]
                    per_archive[key] = per_archive.get(key, 0) + 1
            for archive_path, matched in per_archive.items():
                # A single archive covers "all" generated folders only when it
                # matched every considered entry AND nothing landed outside a
                # tracked archive AND no other tracked archive claimed any
                # folder. Anything else is a partial overlap for that archive.
                full = (
                    unmatched == 0
                    and len(per_archive) == 1
                    and matched == considered
                    and considered > 0
                )
                managed_archives.append({
                    "path": archive_path,
                    "photo_count": _archive_photo_count(archive_path),
                    "coverage": "full" if full else "partial",
                })
            if (
                len(managed_archives) == 1
                and managed_archives[0]["coverage"] == "full"
            ):
                managed_archive = {
                    "path": managed_archives[0]["path"],
                    "photo_count": managed_archives[0]["photo_count"],
                }

        result["managed_archive"] = managed_archive
        # ``managed_archives`` is the authoritative list — the UI should
        # prefer it so partial or multi-archive overlaps are described
        # honestly. ``managed_archive`` stays populated only for the
        # single-archive full-coverage case so older clients keep working.
        result["managed_archives"] = managed_archives
        return jsonify(result)

    @blueprint.route("/api/import/folder-preview/thumbnail")
    def api_import_folder_preview_thumbnail():
        """Generate an on-the-fly thumbnail for a source file (not yet imported).

        Cache policy: only success responses are cacheable. Failures emit
        ``Cache-Control: no-store`` so a transient libraw I/O glitch (NAS
        contention, network blip) doesn't pin question marks in the user's
        preview grid for the cache lifetime — the next page load retries
        and typically succeeds.
        """
        file_path = request.args.get("path", "")
        if not file_path:
            return json_error("path parameter required", 400)
        if not os.path.isfile(file_path):
            resp = make_response("", 404)
            resp.cache_control.no_store = True
            return resp

        from image_loader import load_image
        img = load_image(file_path, max_size=200)
        if img is None:
            resp = make_response("", 404)
            resp.cache_control.no_store = True
            return resp

        import io
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=70)
        buf.seek(0)

        resp = make_response(buf.read())
        resp.content_type = "image/jpeg"
        resp.cache_control.public = True
        resp.cache_control.max_age = 300  # 5 min — these are ephemeral
        return resp

    @blueprint.route("/api/import/readiness")
    def api_import_readiness():
        """Report metadata tooling and repairable degraded-import rows."""
        from image_loader import is_excluded_scan_path
        from metadata import exiftool_status

        db = get_db()
        active_ws = db._active_workspace_id
        status = exiftool_status()
        roots = [r["path"] for r in db.get_workspace_folder_roots(active_ws)]
        # Filter macOS app-managed library bundles before ``os.path.isdir``:
        # stat-ing a ``.photoslibrary`` root (or a symlink into one) itself
        # trips the "access data from other apps" TCC prompt, and this
        # readiness call fires automatically as soon as the Import page
        # opens. See ``api_job_scan`` for the same guard on user-supplied
        # roots; legacy workspace roots need the same treatment.
        reachable_roots = [
            root for root in roots
            if not is_excluded_scan_path(root) and os.path.isdir(root)
        ]
        # Scope the count to reachable roots so an offline drive can't
        # inflate ``metadata_repair_count`` and enable a Repair button
        # for a job that would immediately no-op on the photos it can't
        # touch. When there are no reachable roots the scoped count is
        # 0, which itself gates ``metadata_repair_available`` — no need
        # for the previous explicit ``reachable_roots`` conjunction.
        repair_count = metadata_repair_count(db, active_ws, reachable_roots)
        return jsonify({
            "exiftool": status,
            "requires_exiftool": config["REQUIRE_EXIFTOOL_FOR_IMPORT"],
            "metadata_repair_count": repair_count,
            "metadata_repair_available": bool(
                status["available"] and repair_count
            ),
            "reachable_root_count": len(reachable_roots),
        })

    @blueprint.route("/api/jobs/import-full", methods=["POST"])
    @background_job
    def api_job_import_full(ctx):
        """Full-chain import: copy files -> scan -> create collection."""
        # A full import without an active workspace would copy files and
        # then leave catalog rows invisible to every workspace: the
        # worker's scan calls ``set_active_workspace(None)`` and
        # ``Database.add_folder`` skips the workspace link, and the later
        # ``add_collection()`` call raises because ``_ws_id()`` is
        # unavailable — after the scanner has already committed rows.
        # The mutation-reservation hook deliberately lets no-workspace
        # requests through so routes that answer that state can respond
        # cleanly; this route is not one of them, so refuse here. See
        # the parallel guard at the top of ``api_job_scan``.
        if ctx.workspace_id is None:
            return json_error("no active workspace", 400)
        body = request.get_json(silent=True) or {}
        params = _ImportFullRequest.from_body(body)
        error = params.validation_error()
        if error is not None:
            return json_error(error)
        # The scan below walks the destination (or, in place, the source); a
        # folder-level local copy of any part of that tree would be
        # catalogued a second time at its original path.
        #
        # ``include_descendants=True`` is deliberate even for a copy import.
        # A copy renders each file's destination folder from ``folder_template``
        # and the file's capture time, so an archive that merely *contains* a
        # staged day folder is not automatically safe: a template flat enough
        # to match the staged folder's own path (``%Y-%m-%d`` when
        # ``/archive/2024-05-01`` is staged, plus a photo taken that day) would
        # copy into the original source and then scan it, creating original-path
        # catalog rows alongside the rebased local-copy rows. Refuse the whole
        # import in that case and let the user sync or discard the local copy
        # first, rather than trying to enumerate every ``folder_template``
        # rendering at request time.
        db_for_conflict = get_db()
        runner = get_runner()
        with stage_boundary_lock():
            conflict = _import_scan_conflict(
                runner, db_for_conflict, [params.scan_path], ctx.workspace_id,
            )
        if conflict:
            return json_error(conflict, 409)
        # A folder-stage request that arrives after the pre-flight check
        # above releases ``stage_boundary_lock`` and before the runner
        # registers this import would see no import job and be admitted;
        # this import would then start against a source the stage is
        # rebasing, letting both workers race the same tree. Re-check and
        # register the job atomically under the boundary lock so a stage
        # racing the registration blocks on the same guard its admission
        # takes. See ``local_folder._busy_job`` for the reverse direction.
        _scan_path = [params.scan_path]

        def work(job):
            return _ImportFullRun(
                ctx, params, config, invalidate_missing_originals, job,
            ).run()

        with stage_boundary_lock():
            conflict = _import_scan_conflict(
                runner, db_for_conflict, _scan_path, ctx.workspace_id,
            )
            if conflict:
                return json_error(conflict, 409)
            return ctx.start(
                "import-full", work, pausable=True,
                config={"source": params.source, "destination": params.destination, "copy": params.copy, "file_types": params.file_types},
            )

    @blueprint.route("/api/jobs/import", methods=["POST"])
    @background_job
    def api_job_import(ctx):
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        strategy = body.get("strategy", "merge_all")
        write_xmp = body.get("write_xmp", False)
        if not catalogs:
            return json_error("catalogs required")

        def work(job):
            from importer import execute_import

            thread_db = ctx.thread_db()

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                    },
                )

            return execute_import(
                catalogs,
                thread_db,
                write_xmp=write_xmp,
                strategy=strategy,
                progress_callback=progress_cb,
                pause_callback=lambda: ctx.checkpoint(job),
            )

        return ctx.start(
            "import", work, pausable=True, config={"catalogs": catalogs, "strategy": strategy},
        )

    @blueprint.route("/api/import/orphaned-staging", methods=["GET"])
    def api_import_orphaned_staging():
        """List old pipeline staging folders that need verified recovery."""
        from staging_recovery import discover_orphaned_staging

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        return jsonify({"items": discover_orphaned_staging(vireo_dir)})

    @blueprint.route("/api/import/orphaned-staging/verify", methods=["POST"])
    @background_job
    def api_import_orphaned_staging_verify(ctx):
        """Start a verification job for one old pipeline staging folder."""
        from staging_recovery import verify_orphaned_staging

        body = request.get_json(silent=True) or {}
        path = body.get("path")
        if not isinstance(path, str) or not path:
            return json_error("path required")

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        def work(job):
            thread_db = ctx.thread_db()
            return verify_orphaned_staging(
                thread_db, vireo_dir, path, pause_callback=lambda: ctx.checkpoint(job),
            )

        return ctx.start(
            "staging-verify",
            work,
            pausable=True,
            config={"path": path},
        )

    @blueprint.route("/api/import/orphaned-staging", methods=["DELETE"])
    def api_import_orphaned_staging_delete():
        """Delete old staging only when a fresh verification is fully green."""
        from staging_recovery import delete_verified_staging

        body = request.get_json(silent=True) or {}
        path = body.get("path")
        if not isinstance(path, str) or not path:
            return json_error("path required")
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        db = get_db()
        try:
            result = delete_verified_staging(db, vireo_dir, path)
        except ValueError as exc:
            return json_error(str(exc), status=409)
        return jsonify(result)

    return blueprint
