"""Scanning stages for the streaming photo pipeline.

``scanner_stage`` catalogs the run's photos in one of three ways: a
metadata repair over the collection, an ingest of the sources into the
destination followed by a scan of what landed there, or a scan of each
source in place. ``_ScanPass`` holds the state those phases share (the
worker DB, the roots fed to the scanner and the progress accumulators).
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field

from pipeline_stages.context import PipelineRun

log = logging.getLogger(__name__)


def scanner_stage(
    run: PipelineRun,
    *,
    _SENTINEL,
    _filter_excluded,
    _find_broken_metadata_folders,
    _missing_archive_mount_root,
    _put_scan_item,
    collected_photo_ids,
    effective_thumb_cache_dir,
    effective_vireo_dir,
    final_destination,
    missing_originals_invalidator,
    remote_archive,
    skip_scan,
    snapshot_paths,
):
    # Note: stages["scan"]["status"] is NOT set to "running" here. It is
    # flipped to "running" just before each do_scan() call below, so
    # numScan doesn't pulse during the ingest sub-phase.
    scan = _ScanPass(
        run,
        sentinel=_SENTINEL,
        filter_excluded=_filter_excluded,
        find_broken_metadata_folders=_find_broken_metadata_folders,
        missing_archive_mount_root=_missing_archive_mount_root,
        put_scan_item=_put_scan_item,
        collected_photo_ids=collected_photo_ids,
        effective_thumb_cache_dir=effective_thumb_cache_dir,
        effective_vireo_dir=effective_vireo_dir,
        final_destination=final_destination,
        missing_originals_invalidator=missing_originals_invalidator,
        remote_archive=remote_archive,
        skip_scan=skip_scan,
        snapshot_paths=snapshot_paths,
    )
    try:
        import config as cfg
        from scanner import ScanCancelled
        from scanner import scan as do_scan

        scan.run_all(cfg, do_scan, ScanCancelled)
    except Exception as e:
        if isinstance(e, ScanCancelled) and scan.cancel_requested():
            run.abort.set()
            scan.mark_scan_cancelled()
        else:
            run.errors.append(f"[scan] Fatal: {e}")
            log.exception("Pipeline scan stage failed")
            run.abort.set()
            run.stages["scan"]["status"] = "failed"
            run.runner.update_step(run.job["id"], "scan", status="failed", error=str(e))
    finally:
        scan.finish()


@dataclass
class _ProgressAccumulator:
    """Cumulative (current, total) across one phase's per-folder calls."""

    prior: int = 0
    last_total: int = 0

    def fold(self, current, total):
        self.last_total = total
        return self.prior + current, self.prior + total

    def advance(self):
        self.prior += self.last_total
        self.last_total = 0


@dataclass
class _IngestTotals:
    """What ingest() reported, summed over every source folder."""

    copied_paths: list = field(default_factory=list)
    duplicate_folders: set = field(default_factory=set)
    copied: int = 0
    skipped: int = 0
    failed: int = 0

    def add(self, result_info):
        self.copied_paths.extend(result_info.get("copied_paths", []))
        self.duplicate_folders.update(result_info.get("duplicate_folders", []))
        self.copied += result_info.get("copied", 0)
        self.skipped += result_info.get("skipped_duplicate", 0)
        self.failed += result_info.get("failed", 0)

    def summary(self):
        parts = []
        if self.copied:
            parts.append(f"{self.copied} copied")
        if self.skipped:
            parts.append(f"{self.skipped} skipped")
        if self.failed:
            parts.append(f"{self.failed} failed")
        return ", ".join(parts) or "0 files"


def _rebased_folder_suffix(folder, root_real, dest_ci_root):
    """The part of ``folder`` below the destination root, or None.

    Extract the below-root portion of the folder path so we can rebase
    onto the user-picked root spelling (archive_conflict_report joins
    `path`/rel_folder/filename to build the dest_key we're matching
    against). A realpath-based string prefix covers the same-case and
    symlink-alias cases; the case-fold branch mirrors
    _path_equal_or_descends' probed-CI-root logic so a POSIX case-only
    alias still yields the right tail.
    """
    folder_real = os.path.normcase(os.path.realpath(folder))
    rel_suffix: str | None = None
    if folder_real == root_real:
        rel_suffix = ""
    elif folder_real.startswith(root_real + os.sep):
        rel_suffix = folder_real[len(root_real) + 1:]
    elif dest_ci_root:
        root_low = root_real.lower()
        folder_low = folder_real.lower()
        if folder_low == root_low:
            rel_suffix = ""
        elif folder_low.startswith(root_low + os.sep):
            rel_suffix = folder_real[len(root_real) + 1:]
    return rel_suffix


class _ScanPass:
    """State shared by every phase of one scanner stage run."""

    def __init__(
        self,
        run,
        *,
        sentinel,
        filter_excluded,
        find_broken_metadata_folders,
        missing_archive_mount_root,
        put_scan_item,
        collected_photo_ids,
        effective_thumb_cache_dir,
        effective_vireo_dir,
        final_destination,
        missing_originals_invalidator,
        remote_archive,
        skip_scan,
        snapshot_paths,
    ):
        self.run = run
        self.sentinel = sentinel
        self.filter_excluded = filter_excluded
        self.find_broken_metadata_folders = find_broken_metadata_folders
        self.missing_archive_mount_root = missing_archive_mount_root
        self.put_scan_item = put_scan_item
        self.collected_photo_ids = collected_photo_ids
        self.effective_thumb_cache_dir = effective_thumb_cache_dir
        self.effective_vireo_dir = effective_vireo_dir
        self.final_destination = final_destination
        self.missing_originals_invalidator = missing_originals_invalidator
        self.remote_archive = remote_archive
        self.skip_scan = skip_scan
        self.snapshot_paths = snapshot_paths
        # Dedupes `_on_scanned_photo` by photo_id so a rescan that sees
        # both members of a RAW/JPEG pair (the RAW through its own row,
        # the companion JPEG under the same owner id via
        # ``scanner._credit_known_companion``) only appends the id to
        # ``collected_photo_ids`` once and only queues one thumbnail
        # item for it. Without this, the thumbnail stage would process
        # the same photo twice — whichever path sorts first would win
        # the shared ``{owner_id}.jpg`` output — and the generated
        # pipeline collection would carry duplicate ids. ``_scan``
        # resets this set at the start of every scanner invocation,
        # scoping dedup to one pass so SQLite's reuse of a JPEG photo
        # id that pairing merged away in the next source does not drop a
        # genuinely new photo's callback.
        self._reported_photo_ids: set = set()
        # Mirror of ``collected_photo_ids`` for O(1) membership in
        # ``_on_merged_photo``; the list keeps the collection's order.
        self._collected_ids: set = set()
        # Pairing covers the whole catalog, including ids reported by an
        # earlier source. Keep identity history for the whole stage while
        # the callback dedup set remains scoped to each scanner invocation.
        self._reported_photo_identities = {}

        # Collect the scan roots actually fed to do_scan so the finally
        # clause can invalidate the new-images cache for each one,
        # matching the try/finally pattern used by api_job_scan /
        # api_job_import_full in vireo/app.py. scanner.scan commits photo
        # rows incrementally, so even a mid-scan failure needs
        # invalidation.
        self.scanned_roots: list[str] = []
        self.thread_db = None
        # Accumulator so multi-folder scans (repair loop, scan-in-place
        # with sources=[...]) don't rewind the overall progress at each
        # folder boundary. scan() reports (current, total) local to the
        # invocation; we fold those into cumulative counters that the
        # weighted overall bar reads via stages["scan"].
        self.scan_progress = _ProgressAccumulator()
        # Same accumulator pattern as scan_progress: do_ingest() is
        # called once per source folder, with (current, total) local to
        # each call. Without accumulation, overall progress rewinds at
        # each source boundary.
        self.ingest_progress = _ProgressAccumulator()
        # Directories already reported by _on_permission_denied.
        self.denied_seen: set[str] = set()

    # -- the run -----------------------------------------------------

    def run_all(self, cfg, do_scan, scan_cancelled):
        run = self.run
        self.do_scan = do_scan
        self.scan_cancelled = scan_cancelled

        self.thread_db = run.database_factory(run.db_path)
        self.thread_db.set_active_workspace(run.workspace_id)

        self.effective_cfg = self.thread_db.get_effective_config(cfg.load())
        self.pipeline_cfg = self.effective_cfg.get("pipeline", {})

        if self.skip_scan:
            self._repair_collection_metadata()
            return

        # Determine source folder(s)
        sources = run.params.sources or ([run.params.source] if run.params.source else [])

        if run.params.destination:
            if not self._prepare_import(sources):
                return
            # Copy mode: ingest all sources first, then scan destination
            # subfolders that received files.
            totals = self._ingest_sources(sources)
            if not self._finish_ingest_step(totals):
                return
            self._scan_destination(totals)
        else:
            self._scan_in_place(sources)
        self._finish_scan_step()

    def finish(self):
        """Invalidate caches for every scanned root, then end the queue."""
        run = self.run
        # Invalidate the new-images cache for every root fed to do_scan,
        # on both success and exception paths. scanner.scan commits photo
        # rows incrementally, so even a mid-scan failure can leave DB
        # state that invalidates cached new-image counts. Mirrors the
        # try/finally in api_job_scan and api_job_import_full.
        if self.thread_db is not None and self.scanned_roots:
            from new_images import invalidate_new_images_after_scan
            for scanned_root in self.scanned_roots:
                try:
                    invalidate_new_images_after_scan(self.thread_db, scanned_root)
                except Exception:
                    log.exception(
                        "Failed to invalidate new-images cache for %s",
                        scanned_root,
                    )
                # scanner.scan touches disk and may add or remove
                # photo rows; a ready Missing Originals payload
                # computed before the pipeline scan can now be
                # stale (e.g. user restored an original before
                # running Process). Standalone scan / import jobs
                # already invalidate here — mirror that for
                # pipeline scans so GET /api/photos/missing does
                # not keep serving the pre-scan photo list.
                if self.missing_originals_invalidator is not None:
                    try:
                        self.missing_originals_invalidator()
                    except Exception:
                        log.exception(
                            "Failed to invalidate missing-originals "
                            "cache for %s",
                            scanned_root,
                        )
        self.put_scan_item(self.sentinel)
        run.update_stages(run.runner, run.job["id"], run.stages)

    # -- scanner callbacks and shared step updates -------------------

    def cancel_requested(self):
        run = self.run
        return run.control.should_abort(run.abort) or run.control.cancellation_requested()

    def mark_scan_cancelled(self):
        run = self.run
        run.stages["scan"]["status"] = "skipped"
        run.runner.update_step(
            run.job["id"], "scan", status="completed", summary="Cancelled",
        )

    def _on_scanned_photo(self, photo_id, path):
        run = self.run
        if photo_id in self._reported_photo_ids:
            # The scanner reports an unchanged companion JPEG under
            # its RAW's owner id so import-in-place can tick the
            # companion path off, but the pipeline needs only one
            # entry per photo for the thumbnail queue, the scan count
            # and the collection it builds from these ids.
            return
        self._reported_photo_ids.add(photo_id)
        # When a paired JPEG's callback arrives before its RAW's own
        # callback (file ordering is filesystem-dependent), ``path``
        # is the companion's path, not the canonical RAW. The thumbnail
        # stage keys its output file on the owner id and whichever path
        # it processes first wins the shared ``{owner_id}.jpg``, so a
        # JPEG-first queue entry bypasses the normal RAW-first/fallback
        # selection. Normalize to the catalog's own filename for the id
        # so the queued path is always the RAW's.
        canonical = self._canonical_photo_path(photo_id, path)
        self.collected_photo_ids.append(photo_id)
        self._collected_ids.add(photo_id)
        # Abort/pause-aware: a blocking put would wedge the scanner
        # if the thumbnail consumer parked or failed on a full queue.
        self.put_scan_item((photo_id, canonical))
        run.stages["scan"]["count"] = len(self.collected_photo_ids)
        run.runner.update_step(run.job["id"], "scan",
                               current_file=os.path.basename(canonical))

    def _on_merged_photo(self, old_id, new_id, path):
        """Replace an id the scan's pairing pass merged into a RAW.

        Only a JPEG that was already a photo of its own reaches that pass
        (one cataloged before its RAW arrived); it was reported here
        under its own id, which no longer names a photo and could be given
        to the next insert. The collection and the scan count follow the
        RAW instead. A thumbnail item already queued for ``old_id`` is
        dropped by the thumbnail stage, which checks the row still owns
        the queued path.
        """
        if old_id not in self._collected_ids:
            return
        self._collected_ids.discard(old_id)
        self._reported_photo_ids.discard(old_id)
        self.collected_photo_ids[:] = [
            pid for pid in self.collected_photo_ids if pid != old_id
        ]
        if new_id is None or new_id in self._collected_ids:
            self.run.stages["scan"]["count"] = len(self.collected_photo_ids)
        else:
            self._on_scanned_photo(new_id, path)

    def _canonical_photo_path(self, photo_id, path):
        """Return the catalog's own path for ``photo_id`` (RAW path for pairs).

        When the scanner's companion-credit callback hands us a JPEG path
        whose basename matches the owner's filename, use ``path`` unchanged
        (the common case where companion and owner share a stem is still
        covered by the plain basename compare). When the basename differs,
        the given path belongs to a companion with a different-stem JPEG;
        reconstruct the canonical path from the owner's folder and
        filename so the pipeline downstream (thumbnails, collection,
        current-file display) works on the RAW rather than the companion.

        When the catalog's RAW is unavailable on disk (deleted or
        unreadable) but the companion JPEG is still here, keep the
        given companion path. The thumbnail stage never loads
        ``detail_photo`` for a photo without an edit recipe, so a RAW
        path that fails to decode cannot fall back to the companion from
        there -- reporting the companion directly lets ``_generate``
        process the available file instead of marking the thumbnail
        failed.
        """
        thread_db = self.thread_db
        if thread_db is None:
            return path
        try:
            filenames = thread_db.get_photo_filenames([photo_id])
        except Exception:
            log.exception(
                "Failed to resolve canonical filename for photo %s", photo_id,
            )
            return path
        entry = filenames.get(photo_id)
        if not entry:
            return path
        folder_id, filename = entry
        if os.path.basename(path) == filename:
            return path
        try:
            folder = thread_db.get_folder(folder_id)
        except Exception:
            log.exception(
                "Failed to resolve folder %s for photo %s", folder_id, photo_id,
            )
            return path
        if folder is None:
            return path
        folder_path = folder["path"]
        if not folder_path:
            return path
        canonical = os.path.join(folder_path, filename)
        if not os.path.exists(canonical):
            return path
        return canonical

    def _on_scan_status(self, message, phase_current=None, phase_total=None, phase_label=None):
        run = self.run
        run.runner.update_step(run.job["id"], "scan", current_file=message)
        extra = {"current_file": message}
        if phase_current is not None or phase_total is not None:
            extra.update({
                "phase_current": phase_current,
                "phase_total": phase_total,
                "phase_label": phase_label,
            })
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "scan",
            phase_label or message, **extra,
        )

    def _scan_pause_requested(self):
        # Non-blocking pause probe for scanner.scan. When the
        # pipeline is about to pause, this returns True *before*
        # ``cancel_check`` parks — giving the scanner a chance to
        # terminate its worker processes and release its CPU
        # permits back to the ledger, so replacement scans,
        # model loads, and CPU inference aren't starved for the
        # duration of the pause.
        run = self.run
        probe = getattr(run.runner, "pause_requested", None)
        return bool(probe and probe(run.job["id"]))

    def _on_scan_progress(self, current, total):
        run = self.run
        cum_current, cum_total = self.scan_progress.fold(current, total)
        run.stages["scan"]["count"] = cum_current
        run.stages["scan"]["total"] = cum_total
        elapsed = time.time() - run.job["_start_time"]
        rate = round(cum_current / max(elapsed, 0.01) * 60, 1)  # files/min
        remaining = cum_total - cum_current
        rate_per_sec = cum_current / max(elapsed, 0.01)
        eta = round(remaining / rate_per_sec) if rate_per_sec > 0 and cum_current >= 10 else None
        run.runner.update_step(run.job["id"], "scan",
                               progress={"current": cum_current, "total": cum_total})
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "scan", "Scanning photos",
            rate=rate,
            eta_seconds=eta,
        )

    def _on_permission_denied(self, path: str) -> None:
        """Record a directory the scan walk was refused.

        Surface kernel-level enumeration denials (macOS TCC EPERM,
        POSIX EACCES) into job["errors"]. Without this, scanner.scan
        silently skipped the subtree and the scan stage finished as
        "completed" with 0 photos — a black-box outcome the user
        reads as "no photos found here" when the truth is "Vireo
        was denied access". Dedup per path because the walk may
        signal the same dir more than once.
        """
        if path in self.denied_seen:
            return
        self.denied_seen.add(path)
        self.run.errors.append(
            f"[scan] PERMISSION_DENIED: {path} — macOS or "
            f"filesystem refused enumeration. On macOS open "
            f"System Settings → Privacy & Security → Files "
            f"and Folders (or Removable/Network Volumes) and "
            f"grant Vireo access."
        )

    def _scan(self, root, **kwargs):
        """Run scanner.scan with the callbacks and settings every scan shares."""
        # Scope the callback-id dedup set to one scanner invocation. SQLite
        # reuses the ids of deleted rows, and the pairing pass at the end of
        # a scan can delete a JPEG photo's row after its _on_scanned_photo
        # callback added that id to the set (``_on_merged_photo`` takes it
        # back out). On the next scanner invocation (``_scan_in_place``
        # iterates sources, one do_scan per source), a reused id would be
        # dropped by dedup and miss both collected_photo_ids and the
        # thumbnail queue. A single scanner pass never reuses an id itself
        # (pairing runs at the end), so clearing per-invocation keeps the
        # pair/companion dedup inside one pass intact.
        self._reported_photo_ids.clear()
        self.do_scan(
            root, self.thread_db,
            progress_callback=self._on_scan_progress,
            incremental=True,
            extract_full_metadata=self.pipeline_cfg.get("extract_full_metadata", True),
            status_callback=self._on_scan_status,
            vireo_dir=self.effective_vireo_dir,
            thumb_cache_dir=self.effective_thumb_cache_dir,
            cancel_check=self.cancel_requested,
            pause_check=self._scan_pause_requested,
            cancel_only_check=self.run.control.cancellation_requested,
            reported_photo_identities=self._reported_photo_identities,
            **kwargs,
        )

    def _start_scan_step(self):
        # Flip scan to running and reset job progress so status
        # events during enumeration don't carry ingest's numbers.
        run = self.run
        run.stages["scan"]["status"] = "running"
        run.runner.update_step(run.job["id"], "scan", status="running")
        run.job["progress"]["current"] = 0
        run.job["progress"]["total"] = 0
        run.update_stages(run.runner, run.job["id"], run.stages)

    def _summary_with_metadata_warning(self, summary):
        from metadata import scan_metadata_warning

        metadata_warning = scan_metadata_warning()
        if metadata_warning:
            summary += f" — {metadata_warning}"
        return summary

    # -- collection mode: metadata repair ----------------------------

    def _repair_collection_metadata(self):
        """Repair the collection's broken metadata, or skip the scan.

        Collection mode: no scan targets, but check whether any
        photos in the collection have broken metadata (NULL timestamp
        or RAW thumbnail-sized dimensions) that would poison
        downstream stages. If so, run a targeted repair scan on just
        the affected folders. This is the self-healing path — when
        nothing's broken, we keep the historical "Skipped" summary.
        """
        run = self.run
        coll_photos = self.thread_db.get_collection_photos(
            run.collection_id, per_page=999999,
        )
        # Respect the user's preview-time exclusions: photos removed
        # from this run must not be rescanned or have their metadata
        # rewritten as a side effect of repair.
        in_scope_photos = self.filter_excluded(coll_photos)
        broken = self.find_broken_metadata_folders(
            self.thread_db, [p["id"] for p in in_scope_photos],
        )
        if not broken:
            run.stages["scan"]["status"] = "skipped"
            run.runner.update_step(
                run.job["id"], "scan", status="completed",
                summary="Skipped (using collection)",
            )
            run.update_stages(run.runner, run.job["id"], run.stages)
            self.put_scan_item(self.sentinel)
            return

        total_broken = sum(len(paths) for _, paths in broken)
        run.stages["scan"]["label"] = (
            f"Repair metadata ({total_broken} photos)"
        )
        run.stages["scan"]["status"] = "running"
        run.runner.update_step(
            run.job["id"], "scan", status="running",
            summary=(f"Repairing {total_broken} photos in "
                     f"{len(broken)} folder"
                     f"{'s' if len(broken) != 1 else ''}"),
        )
        run.update_stages(run.runner, run.job["id"], run.stages)

        unreachable = self._repair_folders(broken)

        if self.cancel_requested():
            self.mark_scan_cancelled()
            self.put_scan_item(self.sentinel)
            return

        summary = f"{total_broken} photos repaired"
        if unreachable:
            summary += (f", {unreachable} folder"
                        f"{'s' if unreachable != 1 else ''} unreachable")
        # Mirror the standalone scan/import paths in app.py: append
        # the missing-exiftool warning so a repair scan that lost
        # metadata reads as degraded, not as a clean success.
        summary = self._summary_with_metadata_warning(summary)
        run.stages["scan"]["status"] = "completed"
        run.runner.update_step(
            run.job["id"], "scan", status="completed", summary=summary,
        )
        self.put_scan_item(self.sentinel)

    def _on_repaired_photo(self, photo_id, path):
        # Display-only callback for the repair path: updates the
        # scan step's current_file indicator but does NOT enqueue
        # into scan_to_thumb. In collection mode thumbnail_stage
        # already replays the full collection against the thumb
        # cache, so enqueueing here would double-process every
        # repaired photo and inflate the thumbnail totals.
        run = self.run
        run.runner.update_step(
            run.job["id"], "scan",
            current_file=os.path.basename(path),
        )

    def _repair_folders(self, broken):
        """Rescan each broken folder; return how many were unreachable."""
        run = self.run
        unreachable = 0
        for folder_path, file_paths in broken:
            if not os.path.isdir(folder_path):
                log.warning(
                    "Repair scan skipped for missing folder: %s",
                    folder_path,
                )
                unreachable += 1
                continue
            # Track the repair folder so the outer finally
            # invalidates the Missing Originals cache for it —
            # scanner.scan touches the folder on disk and can
            # revalidate a restored original that a ready
            # /api/photos/missing payload still lists as a
            # ghost. Matches the append pattern used by the
            # normal ingest/scan-in-place paths below.
            self.scanned_roots.append(folder_path)
            try:
                # restrict_files limits discovery to the known
                # broken photos in this folder. Without it, new
                # untracked files in the same folder would get
                # ingested as a side effect of the repair.
                self._scan(
                    folder_path,
                    photo_callback=self._on_repaired_photo,
                    restrict_dirs=[folder_path],
                    restrict_files=set(file_paths),
                    register_restrict_dirs_as_roots=False,
                    allow_photo_inserts=False,
                )
            except (OSError, RuntimeError) as e:
                if isinstance(e, self.scan_cancelled) and self.cancel_requested():
                    run.abort.set()
                    self.mark_scan_cancelled()
                    break
                log.warning(
                    "Repair scan failed for %s: %s", folder_path, e,
                )
                unreachable += 1
            finally:
                self.scan_progress.advance()
        return unreachable

    # -- copy mode: preflight and ingest -----------------------------

    def _prepare_import(self, sources):
        """Load the duplicate oracle and run the local-processing preflight.

        Returns False when the storage preflight refused the run.
        """
        from import_dedup import CatalogIndex
        from ingest import ingest as do_ingest

        self.do_ingest = do_ingest
        # Duplicate-oracle infrastructure shared by the
        # local-processing preflight and the ingest loop. The
        # catalog index is loaded once; every prediction pass
        # gets a FRESH checker over it (seen-state must not
        # leak between predictions or into the real ingest),
        # while the shared times cache keeps each source
        # file's EXIF header read to once per run.
        self.dedup_times_cache: dict = {}
        self.catalog_index = None
        if self.run.params.skip_duplicates:
            self.catalog_index = CatalogIndex.from_db(self.thread_db)

        if self.run.params.local_processing:
            return self._run_storage_preflight(sources)
        return True

    def _fresh_checker(self):
        if self.catalog_index is None:
            return None
        from import_dedup import DuplicateChecker

        return DuplicateChecker(
            self.catalog_index,
            verify_by_hash=self.run.params.verify_by_hash,
            times_cache=self.dedup_times_cache,
        )

    def _ingest_sources(self, sources):
        run = self.run
        run.stages["ingest"]["status"] = "running"
        run.runner.update_step(run.job["id"], "ingest", status="running")
        run.update_stages(run.runner, run.job["id"], run.stages)

        # One shared checker across the whole source loop: files
        # copied by earlier iterations are recorded in it, so
        # later sources treat them as duplicates even before the
        # DB scan (this replaces the old accumulated-hashes
        # re-read of every copied file between sources).
        ingest_checker = self._fresh_checker()
        totals = _IngestTotals()
        for src_folder in sources:
            try:
                result_info = self.do_ingest(
                    source_dir=src_folder,
                    destination_dir=run.params.destination,
                    db=self.thread_db,
                    file_types=run.params.file_types,
                    folder_template=run.params.folder_template,
                    skip_duplicates=run.params.skip_duplicates,
                    progress_callback=self._on_ingest_progress,
                    duplicate_checker=ingest_checker,
                    skip_paths=run.params.exclude_paths,
                    recursive=run.params.recursive,
                )
            finally:
                self.ingest_progress.advance()
            totals.add(result_info)
        return totals

    def _on_ingest_progress(self, current, total, filename):
        run = self.run
        cum_current, cum_total = self.ingest_progress.fold(current, total)
        run.stages["ingest"]["count"] = cum_current
        run.stages["ingest"]["total"] = cum_total
        run.runner.update_step(run.job["id"], "ingest",
                               current_file=filename,
                               progress={"current": cum_current, "total": cum_total})
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "ingest", "Importing photos",
            current_file=filename,
        )

    def _finish_ingest_step(self, totals):
        """Close the ingest step; return False when the run must stop."""
        run = self.run
        # In local-processing mode, ingest failures must fail the
        # ingest stage so archive_stage's "any earlier stage failed"
        # gate skips publishing. ingest() catches per-file copy
        # errors (unreadable source, disk full mid-card) and returns
        # a non-zero ``failed`` count without raising. Without
        # propagating that here the archive step would happily move
        # the partial staging tree to the user's final destination
        # — publishing a partial result that the rest of the
        # pipeline would otherwise treat as a successful import.
        summary = totals.summary()
        if run.params.local_processing and totals.failed:
            msg = (
                f"{totals.failed} file"
                f"{'s' if totals.failed != 1 else ''} failed to copy "
                f"during ingest; archive skipped to avoid publishing "
                f"a partial result"
            )
            run.errors.append(f"[ingest] Fatal: {msg}")
            run.stages["ingest"]["status"] = "failed"
            run.runner.update_step(
                run.job["id"], "ingest",
                status="failed",
                error=msg,
                summary=summary,
            )
            # Stop the run here. Without abort, scanner/previews/
            # classify/regroup would all execute against the
            # partial subset ingest did manage to copy — regroup
            # in particular overwrites the workspace pipeline
            # results with photo IDs that archive_stage's
            # deindex_staging is then going to delete, leaving
            # the workspace pointing at rows that no longer
            # exist. archive_stage already gates on abort and
            # any earlier-stage failure, so this also publishes
            # nothing. Finalize the remaining step rows as
            # skipped so the SSE clients don't see perpetually
            # pending stages.
            run.abort.set()
            run.stages["scan"]["status"] = "skipped"
            run.runner.update_step(
                run.job["id"], "scan",
                status="completed",
                summary="Skipped (ingest failed)",
            )
            run.update_stages(run.runner, run.job["id"], run.stages)
            # The finally clause at the bottom of scanner_stage
            # puts the sentinel on scan_to_thumb so the
            # thumbnail consumer drains and exits.
            return False
        run.stages["ingest"]["status"] = "completed"
        # Ingest is the only stage that ever reads the source
        # (SD card/etc.) — everything after this point works
        # from the copy. Record counts so the UI can tell the
        # user the card is safe to eject instead of leaving
        # them to guess. Only claim this when every discovered
        # file actually made it off the card: local_processing
        # aborts above on any failure, but plain copy mode
        # (local_processing=False) reaches this branch even
        # with total_failed > 0, and the card still holds
        # files that never got copied.
        if totals.failed == 0:
            run.stages["ingest"]["copied"] = totals.copied
            run.stages["ingest"]["skipped_duplicate"] = totals.skipped
            run.result["stages"]["ingest"] = {
                "copied": totals.copied,
                "skipped_duplicate": totals.skipped,
            }
        run.runner.update_step(
            run.job["id"], "ingest", status="completed",
            summary=summary,
        )
        run.update_stages(run.runner, run.job["id"], run.stages)
        return True

    # -- copy mode: local-processing storage preflight ---------------

    def _bail_storage(self, msg):
        # collection_stage spins on stages["scan"]["status"]
        # until it reaches a terminal value, so a storage
        # failure that skips scan and ingest entirely must
        # mark both as skipped here — otherwise its join()
        # blocks the whole pipeline forever.
        run = self.run
        run.errors.append(f"[storage] Fatal: {msg}")
        run.stages["storage"]["status"] = "failed"
        run.runner.update_step(
            run.job["id"], "storage",
            status="failed", error=msg,
        )
        for skipped in ("ingest", "scan"):
            run.stages[skipped]["status"] = "skipped"
            run.runner.update_step(
                run.job["id"], skipped,
                status="completed", summary="Skipped",
            )
        run.abort.set()
        self.put_scan_item(self.sentinel)

    def _run_storage_preflight(self, sources):
        """Check the staging and archive volumes before any copying.

        Returns False after ``_bail_storage`` refused the run.
        """
        from local_processing import format_bytes, selected_source_files

        run = self.run
        run.stages["storage"]["status"] = "running"
        run.runner.update_step(run.job["id"], "storage", status="running")
        run.update_stages(run.runner, run.job["id"], run.stages)

        try:
            if self.remote_archive is not None and not self._check_remote_archive():
                return False
            if not self._check_tracked_destination():
                return False
            archive_parent = archive_space_path = None
            if self.remote_archive is None:
                checked = self._check_local_archive_parent()
                if checked is None:
                    return False
                archive_parent, archive_space_path = checked

            os.makedirs(run.params.destination, exist_ok=True)
            selected_files = selected_source_files(
                sources,
                run.params.file_types,
                recursive=run.params.recursive,
                exclude_paths=run.params.exclude_paths,
            )
            if not self._check_archive_conflicts(selected_files):
                return False
            plan, remote_summary_bits = self._plan_storage(
                selected_files, archive_space_path,
            )
            self._record_storage_plan(plan)
            if plan["batching_required"]:
                self._bail_insufficient_space(plan, archive_parent)
                return False
            summary = (
                f"{format_bytes(plan['required_bytes'])} needed, "
                f"{format_bytes(plan['usable_bytes'])} available"
            )
            if remote_summary_bits:
                summary += "; " + "; ".join(remote_summary_bits)
            run.stages["storage"]["status"] = "completed"
            run.runner.update_step(
                run.job["id"], "storage",
                status="completed",
                summary=summary,
            )
        except Exception as e:
            log.exception("Pipeline local-storage preflight failed")
            self._bail_storage(str(e))
            return False
        return True

    def _check_remote_archive(self):
        """Refuse a remote archive with no GNU rsync or no connection."""
        import move as move_mod

        remote_archive = self.remote_archive
        # Refuse BEFORE staging/processing hours of
        # work, in the same spirit as the local
        # archive-parent checks below: a missing GNU
        # rsync or an unreachable target would
        # otherwise only surface at the final archive
        # move, stranding processed results in
        # staging.
        rsync_bin = move_mod.resolve_rsync_bin(
            self.effective_cfg.get("rsync_bin", "") or "",
        )
        if rsync_bin and not move_mod.is_gnu_rsync(
            rsync_bin,
        ):
            rsync_bin = ""
        if not rsync_bin:
            self._bail_storage(
                "No usable GNU rsync was found for the "
                "remote archive. Install GNU rsync for "
                "your platform or set its executable "
                "under Settings → Paths."
            )
            return False
        conn = move_mod.test_remote_connection(
            remote_archive["target"], rsync_bin,
        )
        if not conn.get("ok"):
            self._bail_storage(
                "Remote archive target "
                f"'{remote_archive['target']['name']}'"
                f" ({remote_archive['display']}) "
                "isn't usable: "
                f"{conn.get('message') or 'connection test failed'}"
            )
            return False
        return True

    def _check_tracked_destination(self):
        """Refuse an archive destination above a tracked folder.

        A tracked archive destination (the import lands at
        or inside a folder Vireo already manages) is no
        longer a hard failure: the archive move opts into
        merging (allow_tracked_merge=True) and folds the
        staged tree into the existing archive. The precise
        per-file content-conflict guard below
        (conflicting_archive_paths) still refuses any
        same-path file whose bytes differ.

        BUT — the merge only supports the "exact overlap"
        (destination IS a tracked folder) and the "ancestor
        overlap" (destination is INSIDE a tracked folder)
        cases. A tracked row STRICTLY BELOW the destination
        (e.g. /Photos/USA already tracked and the user
        picks /Photos) is the "wrap a fresh parent around
        an existing tracked subtree" case which
        move_folder refuses even with allow_tracked_merge.
        Without an early refuse here the pipeline would
        stage and process everything, then fail only at
        the archive step and leave processed results
        stranded under staging. Mirror move_folder's
        alias-folded check so a symlink or case-only alias
        of the tracked path is treated as the exact-match
        case, not a descendant.
        """
        from move import (
            _path_equal_or_descends,
            _tracked_destination_overlap,
        )
        final_destination = self.final_destination
        # For a remote archive, final_destination is the
        # catalog-facing MOUNT path (see where
        # remote_archive is resolved) — the tracked check
        # applies there too, because a prior remote
        # archive to the same target leaves tracked rows
        # at the mount path and the archive move merges
        # into (or refuses around) those exactly like a
        # local destination.
        preflight_tracked = _tracked_destination_overlap(
            self.thread_db, -1, final_destination,
        )
        if preflight_tracked and not _path_equal_or_descends(
            final_destination, preflight_tracked["path"],
        ):
            self._bail_storage(
                f"Archive destination {final_destination} "
                "sits above a folder Vireo already manages "
                f"({preflight_tracked['path']}). Merging "
                "around a tracked subfolder isn't "
                "supported. Pick the tracked folder itself "
                "or a location outside it."
            )
            return False
        return True

    def _check_local_archive_parent(self):
        """Prove a local archive destination can be created and written.

        Returns ``(archive_parent, archive_space_path)``, or None after
        ``_bail_storage`` refused the run.

        Make sure the archive parent exists NOW. Otherwise
        the pipeline would stage and process everything,
        then fail at the final move_folder call when rsync
        tries to write to a missing parent — leaving the
        staged copy stranded under ~/.vireo/staging with no
        archive at the final destination. Nested archive
        targets like /mnt/nas/NewShoot/Photos are the
        common case: the parent /mnt/nas/NewShoot may not
        have been created yet by the user.

        All four checks in this branch are
        local-filesystem-only. For a remote archive
        the destination lives on the NAS: the SSH
        connection test above already proved the
        remote base is a writable directory, and
        move_folder's remote path mkdir-p's the
        subpath parents itself. The local mount
        path deliberately isn't probed — it may
        legitimately be unmounted while archiving
        over SSH (that's the point of this mode).
        """
        final_destination = self.final_destination
        archive_parent = os.path.dirname(
            os.path.normpath(final_destination),
        )
        # Use lexists so a broken/dangling symlink at
        # final_destination is caught here too. os.path
        # .exists returns False for a broken symlink, so
        # a stale link left by an unmounted or moved
        # archive root would slip through, let the
        # pipeline stage and process everything, and
        # only fail when move_folder/rsync tried to
        # create a directory at a path already occupied
        # by that symlink entry.
        if (
            os.path.lexists(final_destination)
            and not os.path.isdir(final_destination)
        ):
            self._bail_storage(
                f"Archive destination {final_destination} "
                "already exists and is not a directory."
            )
            return None
        # Existing archive roots can be mounted volumes; new
        # archive leaves have to probe the existing parent.
        archive_space_path = (
            final_destination
            if os.path.exists(final_destination)
            else archive_parent
        )
        missing_mount_root = (
            self.missing_archive_mount_root(final_destination)
            or self.missing_archive_mount_root(archive_parent)
        )
        if missing_mount_root:
            self._bail_storage(
                f"Archive mount root {missing_mount_root} "
                "is not available. Check that the "
                "destination drive is mounted and writable."
            )
            return None
        try:
            os.makedirs(archive_parent, exist_ok=True)
        except OSError as exc:
            self._bail_storage(
                f"Archive parent {archive_parent} could "
                f"not be created: {exc}. Check that the "
                "destination drive is mounted and writable."
            )
            return None
        return archive_parent, archive_space_path

    def _check_archive_conflicts(self, selected_files):
        """Refuse when the archive holds other files at incoming paths.

        Returns False after ``_bail_storage`` refused the run.
        """
        if self.remote_archive is not None:
            # The conflict report walks the destination
            # tree, which for a remote archive lives on
            # the NAS and isn't locally walkable. The
            # archive move itself runs the equivalent
            # guard over SSH before any file is copied —
            # move_folder's remote merge path probes with
            # ``rsync -an --existing --checksum`` and
            # refuses on any same-path file whose bytes
            # differ — so a conflict still cancels
            # cleanly, just at archive time instead of
            # here.
            return True
        from local_processing import archive_conflict_report

        # When skip_duplicates is on, ingest() will skip
        # sources that duplicate cataloged photos before
        # they ever reach staging. Give the conflict
        # preflight a fresh instance of the same
        # duplicate oracle so a duplicate-source that
        # happens to share an archive path with an
        # unrelated file does not falsely abort the run
        # — ingest will not copy it, so it cannot
        # conflict at archive time.
        archive_report = archive_conflict_report(
            self.final_destination,
            selected_files,
            self.run.params.folder_template,
            duplicate_checker=self._fresh_checker(),
            indexed_paths=self._indexed_archive_paths(
                self.final_destination,
            ),
        )
        archive_conflicts = (
            archive_report["empty"]
            + archive_report["partial"]
            + archive_report["conflicts"]
        )
        if not archive_conflicts:
            return True
        incomplete = (
            archive_report["empty"]
            + archive_report["partial"]
        )
        if incomplete:
            # Only surface incomplete-file paths in
            # this branch: the message tells the user
            # to remove empty/partial debris, so
            # mixing full-content conflict paths into
            # the example list would point them at
            # files that are neither empty nor
            # truncated.
            incomplete_examples = ", ".join(
                incomplete[:3],
            )
            incomplete_more = (
                f" and {len(incomplete) - 3} more"
                if len(incomplete) > 3 else ""
            )
            bits = []
            if archive_report["empty"]:
                bits.append(
                    f"{len(archive_report['empty'])} empty"
                )
            if archive_report["partial"]:
                bits.append(
                    f"{len(archive_report['partial'])} "
                    "partial"
                )
            self._bail_storage(
                "Archive destination contains "
                f"{' and '.join(bits)} unindexed file"
                f"{'s' if len(incomplete) != 1 else ''} "
                "at incoming import paths: "
                f"{incomplete_examples}"
                f"{incomplete_more}. This looks like "
                "an interrupted previous archive "
                "copy. Remove or replace those "
                "incomplete files, then retry; Vireo "
                "will not suffix around likely "
                "corrupt archive files."
            )
            return False
        examples = ", ".join(archive_conflicts[:3])
        more = (
            f" and {len(archive_conflicts) - 3} more"
            if len(archive_conflicts) > 3 else ""
        )
        self._bail_storage(
            "Archive destination already contains "
            "different files at the same import paths: "
            f"{examples}{more}. Pick an empty archive "
            "folder, remove the conflicting files, or "
            "import without local processing."
        )
        return False

    def _indexed_archive_paths(self, root: str) -> set[str]:
        """Cataloged file paths under ``root``, spelled from ``root``.

        Fold symlink/case aliases before deciding a
        cataloged row belongs under this destination:
        the tracked-destination preflight above
        accepts alias-equal roots via
        _path_equal_or_descends, so anything less here
        would drop indexed rows whose stored path uses
        a different alias than the user-picked
        destination (symlink target vs. link, or a
        case-only twin on case-insensitive POSIX like
        default APFS — os.path.normcase is a no-op on
        POSIX and os.path.realpath preserves the
        supplied spelling, so a lexical
        commonpath/is_relative_to check misses the
        case-only alias). Dropping the row would then
        feed an empty index to
        archive_conflict_report and get a
        zero-byte/truncated indexed archive file
        labelled as unindexed failed-copy debris —
        telling the user to remove a cataloged file.
        Probe the case-insensitive fold root once and
        reuse it per row so
        _path_equal_or_descends' listdir/samefile
        probe doesn't re-run per catalog folder.
        """
        from pathlib import Path

        from move import _case_insensitive_root, _path_equal_or_descends

        root_path = Path(os.path.normpath(root))
        root_real = os.path.normcase(
            os.path.realpath(root),
        )
        dest_ci_root = _case_insensitive_root(root)
        indexed: set[str] = set()
        rows = self.thread_db.conn.execute(
            """SELECT f.path, p.filename
                 FROM photos p
                 JOIN folders f ON f.id = p.folder_id"""
        ).fetchall()
        for row in rows:
            folder = row["path"]
            if not _path_equal_or_descends(
                folder, root,
                case_insensitive_root=dest_ci_root,
            ):
                continue
            rel_suffix = _rebased_folder_suffix(folder, root_real, dest_ci_root)
            if rel_suffix is None:
                # _path_equal_or_descends accepted the
                # row via a samefile walk-up (missing
                # intermediate leaf whose parent aliases
                # to root), so the realpath spelling
                # doesn't line up as a string prefix and
                # we can't safely rebase onto the
                # user-picked spelling. Catalog folders
                # exist on disk by construction — this
                # branch is rare — so drop the row
                # rather than fabricate a spelling.
                continue
            indexed_folder = (
                root_path if rel_suffix == ""
                else root_path.joinpath(
                    *rel_suffix.split(os.sep),
                )
            )
            indexed.add(
                str(indexed_folder / row["filename"]),
            )
        return indexed

    def _plan_storage(self, selected_files, archive_space_path):
        """Size the import; return the storage plan and remote summary bits."""
        from local_processing import non_duplicate_files, total_file_bytes

        run = self.run
        planning_files = selected_files
        if (
            run.params.skip_duplicates
            and selected_files
            and self.catalog_index is not None
        ):
            # Plan against the exact files ingest will
            # stage, not the full selection. This keeps
            # both source_bytes and resume credit aligned
            # with skip_duplicates even when the unfiltered
            # plan appears to have enough space.
            planning_files = non_duplicate_files(
                selected_files, self._fresh_checker(),
            )
        source_bytes = total_file_bytes(planning_files)
        if self.remote_archive is not None:
            return self._plan_remote_storage(source_bytes)

        from local_processing import existing_archive_bytes, storage_plan

        # When a previous archive attempt left a partial
        # untracked directory at final_destination, the
        # retry uses move_folder(..., merge=True), which
        # rsyncs only the missing files. Credit the bytes
        # already published so the preflight doesn't
        # reject a retry whose remaining delta would fit.
        existing_bytes = existing_archive_bytes(
            self.final_destination,
            planning_files,
            run.params.folder_template,
        )
        plan = storage_plan(
            run.params.destination, source_bytes,
            archive_parent=archive_space_path,
            archive_existing_bytes=existing_bytes,
        )
        return plan, []

    def _plan_remote_storage(self, source_bytes):
        """Plan staging locally and probe the remote archive's free space.

        Staging-only local plan (the archive volume
        is the NAS, never the same device), then a
        remote df probe for the archive side.
        Probe failures degrade to "check skipped" —
        logged and surfaced in the step summary and
        result payload, never faked as numbers; the
        archive move's own rsync failure is the
        backstop if space actually runs out.
        """
        from local_processing import (
            RESERVED_FREE_BYTES,
            format_bytes,
            storage_plan,
        )
        from move import _remote_free_bytes

        remote_archive = self.remote_archive
        remote_summary_bits = []
        plan = storage_plan(
            self.run.params.destination, source_bytes,
        )
        target = remote_archive["target"]
        # No merge/resume credit for a remote archive:
        # the local resume-credit path (existing_archive_bytes)
        # compares each destination file's size+content
        # against the source, but a remote equivalent
        # would need a per-file walk over SSH. A
        # whole-tree `du` reports every byte at the
        # path — including unrelated files or stale
        # partials that rsync --ignore-existing will
        # still copy past — which could cancel out
        # source_bytes and let the preflight pass on
        # a nearly-full NAS. Budget the full source
        # here; a retry whose remaining delta would
        # actually fit but the full source wouldn't
        # is a rare batch-reject we take over the
        # false-positive that lets processing burn
        # hours before the transfer fails on space.
        archive_delta = source_bytes
        plan["archive_existing_bytes"] = 0
        plan["archive_required_bytes"] = archive_delta
        # df the configured base (just verified as an
        # existing writable dir by the connection
        # test) rather than the not-yet-created leaf.
        remote_free = _remote_free_bytes(
            target, target["remote_path"],
        )
        plan["archive_free_bytes"] = remote_free
        if remote_free is None:
            log.warning(
                "Couldn't probe free space at %s; "
                "skipping remote free-space check",
                remote_archive["display"],
            )
            remote_summary_bits.append(
                "remote free-space check skipped "
                "(probe failed)"
            )
            plan["archive_usable_bytes"] = None
        else:
            archive_usable = max(
                0, remote_free - RESERVED_FREE_BYTES,
            )
            plan["archive_usable_bytes"] = archive_usable
            plan["archive_enough"] = (
                archive_delta <= archive_usable
            )
            plan["enough"] = (
                plan["staging_enough"]
                and plan["archive_enough"]
            )
            plan["batching_required"] = not plan["enough"]
            remote_summary_bits.append(
                f"{format_bytes(remote_free)} free at "
                f"{target['name']}"
            )
        return plan, remote_summary_bits

    def _record_storage_plan(self, plan):
        run = self.run
        run.result["local_processing"] = {
            **plan,
            "staging_destination": run.params.destination,
            "final_destination": self.final_destination,
        }
        if self.remote_archive is not None:
            target = self.remote_archive["target"]
            run.result["local_processing"]["remote"] = {
                "target_id": target["id"],
                "target_name": target["name"],
                "host": target["host"],
                "user": target["user"],
                "ssh_destination": self.remote_archive["ssh_final"],
                "free_space_checked": (
                    plan["archive_free_bytes"] is not None
                ),
            }

    def _bail_insufficient_space(self, plan, archive_parent):
        from local_processing import format_bytes

        # Tell the user which volume came up short — the
        # destination running out of room reads as a
        # different problem (pick a bigger archive
        # drive) than the staging volume running out
        # (free space on ~/.vireo or batch later).
        if not plan.get("archive_enough", True):
            if self.remote_archive is not None:
                self._bail_storage(
                    "Remote archive needs about "
                    f"{format_bytes(plan['archive_required_bytes'])}, "
                    "but only "
                    f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                    "is free at "
                    f"{self.remote_archive['display']} after "
                    "the free-space reserve. Free space "
                    "on the remote volume or pick a "
                    "different target or subpath."
                )
            else:
                self._bail_storage(
                    "Archive destination needs about "
                    f"{format_bytes(plan['archive_required_bytes'])}, "
                    "but only "
                    f"{format_bytes(plan['archive_usable_bytes'] or 0)} "
                    f"is free under {archive_parent} after "
                    "the free-space reserve. Free space at "
                    "the destination or pick a different "
                    "archive folder."
                )
        else:
            self._bail_storage(
                "Local processing needs about "
                f"{format_bytes(plan['required_bytes'])}, but "
                f"only {format_bytes(plan['usable_bytes'])} is "
                "available after keeping local free-space "
                "reserve. This import needs "
                f"{plan['batch_count']} local-processing "
                "batches; automatic batch execution is not "
                "available in this build yet."
            )

    # -- the scans ---------------------------------------------------

    def _destination_restrict_dirs(self, totals):
        """The destination subfolders the copy-mode scan should walk.

        Scan only the destination subfolders that actually contain
        files we care about, not the entire destination tree. Use
        restrict_dirs so the scanner still roots the folder hierarchy
        at the destination, preserving parent folder links. Include
        folders that received copies AND folders that already hold
        duplicates of the source files — both need to be linked to
        the active workspace. Guard every candidate at this seam:
        scanner._ensure_folder recurses parents until it equals the
        scan root; a non-descendant path would recurse all the way
        to '/', so restrict_dirs must contain only descendants of
        params.destination. ingest() already enforces this, but
        we re-check here to keep the invariant local and obvious.
        Both sides are lexically normalized via os.path.normpath so
        a stored path containing ``..`` can't defeat the check.
        """
        from pathlib import Path

        dest_p = Path(os.path.normpath(self.run.params.destination))

        def _under_destination(path: str) -> bool:
            return Path(os.path.normpath(path)).is_relative_to(dest_p)

        restrict_set: set[str] = set()
        if totals.copied_paths:
            restrict_set.update(
                str(Path(p).parent) for p in totals.copied_paths
                if _under_destination(str(Path(p).parent))
            )
        restrict_set.update(
            f for f in totals.duplicate_folders if _under_destination(f)
        )
        return sorted(restrict_set) if restrict_set else None

    def _scan_destination(self, totals):
        restrict = self._destination_restrict_dirs(totals)
        self._start_scan_step()
        self.scanned_roots.append(self.run.params.destination)
        self._scan(
            self.run.params.destination,
            photo_callback=self._on_scanned_photo,
            photo_merged_callback=self._on_merged_photo,
            restrict_dirs=restrict,
            permission_error_callback=self._on_permission_denied,
        )

    def _scan_in_place(self, sources):
        """Scan-in-place: scan each source folder independently."""
        run = self.run
        self._start_scan_step()
        # Snapshot-scoped: hand the scanner the exact file set
        # captured at snapshot time so a file that landed in the
        # folder AFTER the snapshot doesn't get cataloged here.
        # Without this, the scan walks the whole folder, commits
        # a photos row for the late arrival, then the collection
        # stage filters it out of downstream work AND the finally
        # block invalidates the new-images cache — orphaning the
        # file (cataloged in DB, never classified, never
        # re-surfaced by a later banner probe). Skipping it at
        # scan time keeps it uncataloged so the next probe
        # rediscovers it.
        snapshot_files_set = (
            set(self.snapshot_paths) if self.snapshot_paths is not None else None
        )
        for src_folder in sources:
            self.scanned_roots.append(src_folder)
            snapshot_restrict_dirs = None
            snapshot_restrict_files = None
            if snapshot_files_set is not None:
                src_norm = os.path.normpath(src_folder)
                prefix = (
                    src_norm if src_norm.endswith(os.sep)
                    else src_norm + os.sep
                )
                files_under_src = [
                    p for p in self.snapshot_paths
                    if os.path.normpath(p).startswith(prefix)
                ]
                snapshot_restrict_files = set(files_under_src)
                snapshot_restrict_dirs = sorted(
                    {os.path.dirname(p) for p in files_under_src}
                )
            try:
                self._scan(
                    src_folder,
                    photo_callback=self._on_scanned_photo,
                    photo_merged_callback=self._on_merged_photo,
                    skip_paths=run.params.exclude_paths,
                    recursive=run.params.recursive,
                    restrict_dirs=snapshot_restrict_dirs,
                    restrict_files=snapshot_restrict_files,
                    permission_error_callback=self._on_permission_denied,
                )
            finally:
                self.scan_progress.advance()

    def _finish_scan_step(self):
        run = self.run
        if self.cancel_requested():
            self.mark_scan_cancelled()
            return
        run.stages["scan"]["status"] = "completed"
        # ``_on_scan_progress`` reports file-progress counts, so a RAW+JPEG
        # pair that attaches rowlessly to the RAW leaves ``stages["scan"]
        # ["count"]`` reading two files for one photo. The collection the
        # pipeline runs on is ``collected_photo_ids``: take the summary from
        # there (and reset the stage count) so "N photos" matches it.
        photo_count = len(self.collected_photo_ids)
        run.stages["scan"]["count"] = photo_count
        # Pipeline scans use scanner.scan exactly like the standalone
        # /api/jobs/scan path, so a missing exiftool silently strips
        # capture dates, GPS, and camera info here too. Append the
        # same warning the standalone path appends.
        scan_summary = self._summary_with_metadata_warning(
            f"{photo_count} photos"
        )
        run.runner.update_step(run.job["id"], "scan", status="completed",
                               summary=scan_summary)


def _collection_stage_body(
    run: PipelineRun,
    *,
    collected_photo_ids,
    skip_scan,
    snapshot_paths,
):

    if skip_scan:
        return

    # Wait for scanner to complete (don't check abort -- we want the
    # collection regardless so the user can see scanned photos)
    while True:
        # Pause is different from abort: this worker can park while it
        # waits without giving up the collection the scanner produced.
        run.control.pause_checkpoint()
        if run.stages["scan"]["status"] in ("completed", "failed", "skipped"):
            break
        time.sleep(0.1)

    # Snapshot-scoped runs: resolve the captured file paths to photo IDs
    # now that the scanner has committed rows, and trim the collection
    # to exactly that set. The scan stage already restricted the walk
    # to the snapshot's file set via ``restrict_dirs`` + ``restrict_files``
    # so late arrivals aren't cataloged in the first place; this filter
    # is a belt-and-suspenders trim in case a pre-existing (already
    # cataloged) photo somehow ends up in ``collected_photo_ids``. Any
    # snapshot path that never resolved (file was moved/deleted between
    # snapshot and pipeline run) is logged so an unexpectedly small
    # collection is auditable.
    if snapshot_paths is not None:
        resolver_db = run.database_factory(run.db_path)
        resolver_db.set_active_workspace(run.workspace_id)
        # Split each snapshot path into (dirname, basename) and match on
        # the two columns directly. Concatenating with a hardcoded '/'
        # would mismatch Windows paths captured via os.path.join, where
        # both the snapshot and folders.path use backslash separators.
        pairs = [os.path.split(p) for p in snapshot_paths]
        resolved: set[int] = set()
        # 2 placeholders per pair; cap below SQLite's default 999-param
        # limit (pre-3.32) with headroom.
        _CHUNK = 400
        for i in range(0, len(pairs), _CHUNK):
            chunk = pairs[i : i + _CHUNK]
            values = ",".join("(?, ?)" for _ in chunk)
            flat_params = tuple(v for pair in chunk for v in pair)
            rows = resolver_db.conn.execute(
                f"""SELECT p.id
                      FROM photos p
                      JOIN folders f ON f.id = p.folder_id
                     WHERE (f.path, p.filename) IN (VALUES {values})""",
                flat_params,
            ).fetchall()
            resolved.update(r["id"] for r in rows)
        snapshot_photo_ids = resolved

        missing = len(snapshot_paths) - len(snapshot_photo_ids)
        log.info(
            "pipeline: snapshot %s had %d files, %d ingested, %d missing on disk",
            run.params.source_snapshot_id,
            len(snapshot_paths),
            len(snapshot_photo_ids),
            missing,
        )

        # Filter collected_photo_ids to the snapshot set. collected_photo_ids
        # is only read by this stage (to build the collection); the thumbnail
        # queue has already drained it independently.
        collected_photo_ids[:] = [
            pid for pid in collected_photo_ids if pid in snapshot_photo_ids
        ]

    if not collected_photo_ids:
        return

    thread_db = run.database_factory(run.db_path)
    thread_db.set_active_workspace(run.workspace_id)
    from datetime import datetime as dt

    name = "Pipeline " + dt.now().strftime("%Y-%m-%d %H:%M")
    run.collection_id = thread_db.add_collection(
        name,
        json.dumps([{"field": "photo_ids", "value": collected_photo_ids}]),
    )
    run.result["collection_id"] = run.collection_id


def collection_stage(
    run: PipelineRun,
    *,
    _collection_stage_body,
    collection_ready,
):
    """Wait for scan to finish, build collection, signal classifier."""
    # Every exit must set ``collection_ready``: detect_stage waits on it
    # unconditionally, so an exception that skipped the set (e.g. the
    # snapshot resolver hitting a locked DB) would hang the job and
    # pin one of the pipeline slots forever. A failure is recorded as a
    # failed ``collection`` stage so the job reports failed rather than
    # completing with every downstream stage "Skipped".
    try:
        _collection_stage_body()
    except Exception as e:
        run.errors.append(f"[collection] Fatal: {e}")
        log.exception("Pipeline collection stage failed")
        run.abort.set()
        run.stages["collection"] = {
            "status": "failed",
            "label": "Building collection",
            "error": str(e),
        }
        run.update_stages(run.runner, run.job["id"], run.stages)
    finally:
        collection_ready.set()
