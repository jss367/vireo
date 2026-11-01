"""App-construction and background maintenance passes.

``create_app`` runs a handful of self-healing passes around startup: the
ordered catalog repairs (``run_catalog_repairs``), the background thread that
imports keywords embedded in already-scanned files and then marks species,
the thumb_path, label-identity
and EXIF search backfill jobs, and the teardown that stops the job runner
and closes the startup database. ``StartupTasks`` owns them because they
share per-app state (the app, its database path and the startup connection).

This module decides *what* each pass does. Initial scheduling (the
``VIREO_DISABLE_STARTUP_BACKFILL_TIMERS`` guards, thread and timer start
points, delays and daemon flags) stays in ``create_app`` so startup ordering is
readable in one place. Retained-cache maintenance owns its recurring timer
so teardown can cancel it before stopping workers.

``metadata_repair_count`` (the Import page's repair readiness count) and
``utc_iso_now`` are plain functions with no per-app state.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from db import Database
from job_runner.lineage import JobLineage
from services import workspace_config

if TYPE_CHECKING:
    from jobs import JobRunner, LogBroadcaster
    from services.preview_workers import PreviewWorkers
    from services.service_types import JobDict, JobResult, JsonDict

log = logging.getLogger(__name__)


class StartupApp(Protocol):
    """The members of the Flask app ``StartupTasks`` reads (set by ``create_app``)."""

    # Any: the Flask app config holds heterogeneous settings values.
    @property
    def config(self) -> Mapping[str, Any]: ...

    @property
    def _job_runner(self) -> JobRunner: ...

    @property
    def _log_broadcaster(self) -> LogBroadcaster: ...

    @property
    def _preview_workers(self) -> PreviewWorkers: ...


def _label_identity_backfill_is_noop(result: Mapping[str, Any]) -> bool:
    """Whether the label-identity backfill run should be dropped from history.

    A successfully processed label set stamps a repair marker in
    ``apply_label_set`` and records the labels iNaturalist did not match,
    so a run that reaches ``label_sets > 0`` has changed state even when
    no name matched. ``unidentified_labels_total`` is the user-visible
    "47 labels without a single matching iNaturalist species" count, so
    producing one is not a no-op either.
    """
    return not (
        result.get("label_sets")
        or result.get("labels_identified")
        or result.get("predictions_updated")
        or result.get("legacy_predictions_updated")
        or result.get("unidentified_labels_total")
        or result.get("errors")
    )


class StartupTasks:
    """Startup and maintenance passes bound to one app.

    ``app`` is read at call time (``app.config["THUMB_CACHE_DIR"]``,
    ``app._job_runner``, ``app._log_broadcaster``), so the instance can be
    built before those attributes are set. ``init_db`` is the startup
    connection ``create_app`` opened; ``db_path`` is used to open the
    short-lived connections background passes need.
    """

    def __init__(self, app: StartupApp, db_path: str, init_db: Database) -> None:
        self._app = app
        self._db_path = db_path
        self._init_db = init_db
        self._retained_cache_timer: threading.Timer | None = None
        self._retained_cache_stopped = threading.Event()

    def schedule_retained_cache_maintenance(self, delay: float = 10.0) -> None:
        """Visible, periodic maintenance, with work kept off request threads."""
        if self._retained_cache_stopped.is_set():
            return
        def launch() -> None:
            if self._retained_cache_stopped.is_set():
                return
            try:
                self._app._job_runner.start_singleton(
                    "retained-cache-maintenance", self._maintain_retained_caches,
                    workspace_id=None, singleton_key="retained-cache-maintenance",
                    lineage=JobLineage.system(
                        "Hourly cleanup of generated files (previews, working "
                        "copies, analysis caches) Vireo can rebuild"
                    ),
                )
            except Exception:
                log.exception("Could not start retained generated-file maintenance")
            if not self._retained_cache_stopped.is_set():
                self.schedule_retained_cache_maintenance(3600.0)

        self._retained_cache_timer = threading.Timer(delay, launch)
        self._retained_cache_timer.daemon = True
        if self._retained_cache_stopped.is_set():
            self._retained_cache_timer.cancel()
            return
        self._retained_cache_timer.start()

    def _maintain_retained_caches(self, job: JobDict) -> JsonDict:
        import config as cfg
        from catalog_result_files import result_files
        from services.result_files import delete_workspace_results
        from services.retained_cache import cleanup, source_verifier

        runner = self._app._job_runner
        def checkpoint() -> None:
            if runner.is_cancelled(job["id"]):
                raise RuntimeError("Storage maintenance cancelled")
        def io_checkpoint() -> None:
            checkpoint()
            runner.wait_for_background_io(job["id"])
        def progress(current: int, total: int, path: str) -> None:
            runner.push_event(job["id"], "progress", {
                "current": current, "total": total,
                "current_file": os.path.basename(path), "phase": "Storage maintenance",
            })

        io_checkpoint()
        settings = {**cfg.load(), "THUMB_CACHE_DIR": self._app.config["THUMB_CACHE_DIR"],
                    "COMPUTATION_CACHE_DIR": self._app.config["COMPUTATION_CACHE_DIR"]}
        with Database(self._db_path) as db:
            result = cleanup(settings, db.storage.source_folder_paths(),
                             source_is_readable=source_verifier(
                                 db, settings, checkpoint, read_checkpoint=io_checkpoint),
                             checkpoint=io_checkpoint, progress=progress)
            workspaces = db.workspaces.list_all()
            retired_total: dict[str, int] = {}
            for workspace in workspaces:
                io_checkpoint()
                db.set_active_workspace(workspace["id"])
                retired = db._prune_edit_history()
                for key, count in retired.items():
                    result[key] = result.get(key, 0) + count
                    retired_total[key] = retired_total.get(key, 0) + count
            result_workspace_ids = {ws_id for _path, _kind, ws_id in result_files(os.path.dirname(self._db_path))}
            orphans_removed: list[str] = []
            for ws_id in result_workspace_ids:
                io_checkpoint()
                # A workspace may have been created while the preceding
                # maintenance passes were running. Check current ownership
                # rather than deleting from the earlier workspace snapshot.
                if db.workspaces.get(ws_id) is None:
                    errors = delete_workspace_results(
                        os.path.dirname(self._db_path), ws_id, orphans_removed,
                    )
                    result["errors"].extend(errors)
                    result["errors_total"] += len(errors)
                    result["ok"] = result["ok"] and not errors
            result["orphan_results_removed"] = len(orphans_removed)
            result["noop"] = retained_maintenance_noop(result, retired_total)
            return result

    def run_catalog_repairs(self) -> None:
        """Run the ordered startup catalog repairs.

        Each pass is idempotent and cheap when there is nothing to repair.
        ``create_app`` calls this once, on the startup connection, before
        registering any route.
        """
        init_db = self._init_db
        # File-backed startup skips Database's schema initialization. Repair old
        # move parentage here too, before Browse can serve the stale hierarchy.
        init_db.repair_stale_folder_parents()
        repaired_location_ancestors = init_db.repair_misclassified_location_ancestors()
        if repaired_location_ancestors:
            log.info(
                "Restored %d location hierarchy nodes misclassified as taxonomy",
                repaired_location_ancestors,
            )
        # After the location repair, which retypes place nodes that legacy
        # marking had made taxonomy, so a restored place is no longer a twin.
        try:
            init_db.merge_taxonomy_root_twins()
        except Exception:
            log.exception("Could not merge top-level species keyword twins")
        workspace_config.create_default_collections_for_all_workspaces(init_db)
        self.prune_collection_ids_of_missing_photos()

    def prune_collection_ids_of_missing_photos(self) -> list[JsonDict]:
        """Take photos that no longer exist out of every static collection.

        Every boot, not once behind a marker: deletes are covered at the
        source (``repositories.photo_row_deletion``), but a collection can
        still be saved with an id list captured before a delete (the filter
        bar or collection editor submitting rules it loaded earlier, a job
        saving the ids it gathered while photos were being deleted), and no
        delete-side hook sees that. The check reads every collection's rules
        and the catalog's photo ids, about 40 ms on a 100k-photo catalog
        with 90k collection entries, and writes nothing when there is
        nothing to remove. A hit is logged per collection, so a path that
        still leaks shows up in the log.
        """
        db = self._init_db
        started_at = time.time()
        try:
            pruned = db.prune_collection_ids_of_missing_photos()
        except Exception:
            log.exception("Could not check collections for deleted photos")
            db.rollback()
            return []
        if not pruned:
            return []
        for entry in pruned:
            log.info(
                "Removed %d deleted photo(s) from collection %d %r (workspace %s)",
                entry["removed"], entry["collection_id"], entry["name"],
                entry["workspace_id"],
            )
        log.warning(
            "Removed %d entries for deleted photos from %d collection(s) in %.2fs",
            sum(entry["removed"] for entry in pruned), len(pruned),
            time.time() - started_at,
        )
        return pruned

    def mark_species_keywords(self, db: Database, log_label: str) -> None:
        """Load taxonomy and type keywords that name a species."""
        from taxonomy import load_local_taxonomy

        tax = load_local_taxonomy()
        if tax is None:
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.keyword_taxa.mark_species(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
        except Exception:
            log.debug(
                "[%s] species marking failed", log_label, exc_info=True,
            )

    def mark_species(self) -> None:
        """Background keyword backfill and species marking on its own connection.

        The embedded-keyword backfill runs first so the species leaves it
        imports are typed by the marking pass right after it.
        """
        bg_db = None
        try:
            bg_db = Database(self._db_path)
        except Exception:
            log.debug("Could not open background db for species marking", exc_info=True)
            return
        try:
            self.backfill_embedded_keywords(bg_db)
            self.mark_species_keywords(bg_db, "background")
        finally:
            bg_db.close()

    def backfill_embedded_keywords(self, db: Database) -> int:
        """Import keywords Lightroom wrote inside already-scanned JPEG/DNG files.

        One-shot and ``db_meta``-gated (see ``scanner.backfill_embedded_keywords``):
        it reads the ExifTool output stored per photo, never the originals. Kept
        off the startup path because a large catalog has hundreds of candidate
        photos, each a few writes. A failure leaves the marker unset, so the
        next start retries.
        """
        from scanner import backfill_embedded_keywords

        started_at = time.time()
        try:
            imported = backfill_embedded_keywords(db)
        except Exception:
            log.exception("Embedded keyword backfill failed; will retry next start")
            db.rollback()
            return 0
        if imported:
            log.info(
                "Imported embedded keywords onto %d photos in %.2fs",
                imported, time.time() - started_at,
            )
        return imported

    def cleanup_app_resources(self, job_timeout: float = 10.0) -> bool:
        """Stop background jobs, uninstall the log broadcaster, close init_db.

        Returns whether the job runner shut down within ``job_timeout``.
        """
        app = self._app
        self._retained_cache_stopped.set()
        if self._retained_cache_timer:
            self._retained_cache_timer.cancel()
        app._preview_workers.close()
        try:
            jobs_stopped = app._job_runner.shutdown(timeout=job_timeout)
        except Exception:
            jobs_stopped = False
            log.exception("Failed to shut down background jobs cleanly")
        try:
            app._log_broadcaster.uninstall()
        except Exception:
            log.exception("Failed to uninstall log broadcaster during cleanup")
        try:
            self._init_db.close()
        except Exception:
            log.exception("Failed to close database during cleanup")
        return jobs_stopped

    def kickoff_thumb_path_backfill(self) -> None:
        """Start the thumb_path backfill as a system job when it has work.

        The dashboard's coverage card counts thumbnails by ``thumb_path IS
        NOT NULL``, but for a long stretch the column was never populated by
        production code, so libraries with 40k JPEGs cached on disk reported
        "0 thumbnails" forever. This pass aligns the column with disk reality
        for legacy rows, and clears it for photos whose cached file has since
        been deleted (drift correction). It runs as a system JobRunner job
        (``JobLineage.system``), so it shows in the bottom panel and Jobs page
        with the reason it started and its history row survives a crash; a
        run that changed nothing is dropped from history. It is skipped
        entirely when a fast count check finds nothing to do.
        """
        from thumbnails import (
            backfill_thumb_paths,
            thumb_path_backfill_candidate_count,
        )

        app = self._app
        db_path = self._db_path
        init_db = self._init_db
        tpdb = None
        try:
            tpdb = Database(db_path)
            candidate_count = thumb_path_backfill_candidate_count(
                tpdb, app.config["THUMB_CACHE_DIR"],
            )
        except Exception:
            log.exception("thumb_path backfill: candidate check failed")
            return
        finally:
            if tpdb is not None:
                tpdb.close()
        if candidate_count == 0:
            log.debug("thumb_path backfill: no candidates, skipping")
            return

        runner = app._job_runner
        cache_dir = app.config["THUMB_CACHE_DIR"]

        def work(job: JobDict) -> JobResult:
            thread_db = Database(db_path)
            try:
                active_ws = init_db.active_workspace_id
                if active_ws is not None:
                    thread_db.set_active_workspace(active_ws)

                def progress_cb(current: int, total: int) -> None:
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    runner.push_event(
                        job["id"],
                        "progress",
                        {
                            "current": current,
                            "total": total,
                            "phase": f"{current:,} / {total:,} photos reconciled",
                        },
                    )

                def status_cb(message: str, **_phase: object) -> None:
                    runner.push_event(job["id"], "progress", {
                        "phase": message,
                        "current": job["progress"].get("current", 0),
                        "total": job["progress"].get("total", 0),
                    })

                def cancel_check() -> bool:
                    return runner.is_cancelled(job["id"])

                result = backfill_thumb_paths(
                    thread_db, cache_dir,
                    progress_callback=progress_cb,
                    status_callback=status_cb,
                    cancel_check=cancel_check,
                )
                result["noop"] = not (result.get("set") or result.get("cleared"))
                return result
            finally:
                thread_db.close()

        try:
            runner.start(
                "thumb_path_backfill", work,
                config={"trigger": "startup"},
                lineage=JobLineage.system(
                    f"At startup, {candidate_count:,} photo records did not match "
                    "the thumbnails cached on disk"
                ),
            )
        except Exception:
            log.exception("Failed to start thumb_path backfill job")

    def kickoff_label_identity_backfill(self) -> None:
        """Recover taxon IDs for predictions made from legacy label lists.

        Lists saved before they recorded identities leave BioCLIP predictions
        identified by name only, so a species whose name the taxonomy calls
        ambiguous shows up twice in review. See ``label_source_identities``.
        The pass re-queries iNaturalist, so it runs as a system JobRunner job
        (visible in the bottom panel and Jobs page) and only when a label set needs it;
        a list it could not reach stays pending for the next startup.
        """
        from label_source_identities import backfill, legacy_pass_needed, pending_label_sets

        app = self._app
        db_path = self._db_path
        check_db = None
        try:
            check_db = Database(db_path)
            pending = pending_label_sets(check_db) or legacy_pass_needed(check_db)
        except Exception:
            log.exception("Label list species IDs: pending check failed")
            return
        finally:
            if check_db is not None:
                check_db.close()
        if not pending:
            log.debug("Label list species IDs: nothing to recover, skipping")
            return

        runner = app._job_runner

        def work(job: JobDict) -> JobResult:
            thread_db = Database(db_path)
            try:
                def progress(current: int, total: int, phase: str) -> None:
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    runner.push_event(job["id"], "progress", {
                        "current": current, "total": total, "phase": phase,
                    })

                result = backfill(
                    thread_db, progress=progress,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
                )
                result["noop"] = _label_identity_backfill_is_noop(result)
                return result
            finally:
                thread_db.close()

        try:
            runner.start(
                "label-list-species-ids", work,
                config={"trigger": "startup"},
                lineage=JobLineage.system(
                    "At startup, predictions from a label list were missing "
                    "their iNaturalist species IDs"
                ),
            )
        except Exception:
            log.exception("Failed to start label list species ID job")


    def kickoff_exif_search_backfill(self) -> None:
        """Store searchable EXIF values for photos written before the triggers.

        Metadata search walks a photo's EXIF only when its stored tag values
        (``photo_exif_search_text``) contain the term; a photo without a row
        falls back to the slower raw-text check. Triggers cover every later
        EXIF write, so this pass only runs after an upgrade, or after a
        definition change empties the table. A system JobRunner job, so it
        shows in the bottom panel and Jobs page; skipped when nothing is unindexed.
        """
        db_path = self._db_path
        check_db = None
        try:
            check_db = Database(db_path)
            total = check_db.exif_search.count_unindexed()
        except Exception:
            log.exception("EXIF search backfill: pending check failed")
            return
        finally:
            if check_db is not None:
                check_db.close()
        if not total:
            log.debug("EXIF search backfill: nothing to index, skipping")
            return

        runner = self._app._job_runner

        def work(job: JobDict) -> JsonDict:
            thread_db = Database(db_path)
            try:
                indexed = 0
                after_id = 0
                job["progress"]["total"] = total
                while not runner.is_cancelled(job["id"]):
                    batch = thread_db.exif_search.index_batch(after_id, 500)
                    if batch is None:
                        break
                    after_id, count = batch
                    indexed += count
                    # Photos added meanwhile are indexed by the triggers, so
                    # the pass can index fewer than it counted, never more.
                    job["progress"]["current"] = min(indexed, total)
                    runner.push_event(job["id"], "progress", {
                        "current": job["progress"]["current"],
                        "total": total,
                        "phase": f"Indexing photo metadata for search: "
                                 f"{job['progress']['current']:,} / {total:,} photos",
                    })
                log.info("EXIF search backfill: indexed %d photos", indexed)
                return {"indexed": indexed, "noop": not indexed}
            finally:
                thread_db.close()

        try:
            runner.start(
                "exif_search_backfill", work,
                config={"trigger": "startup"},
                lineage=JobLineage.system(
                    f"At startup, {total:,} photos had camera metadata not yet "
                    "indexed for search"
                ),
            )
        except Exception:
            log.exception("Failed to start EXIF search backfill job")


# Maintenance result fields that describe ordinary cache use rather than
# anything the pass did or could not do: what is left, and files kept
# because they were opened or rewritten since the walk.
_MAINTENANCE_CONTEXT_FIELDS = frozenset({
    "remaining_bytes", "kept_accessed_files", "kept_changed_files", "ok", "noop",
})


def retained_maintenance_noop(result: JsonDict, retired: Mapping[str, int]) -> bool:
    """Whether a storage-maintenance pass removed, retired and reported nothing.

    Fails safe: any nonzero count or nonempty list in the result counts as
    something to report unless it is ordinary-use context
    (``_MAINTENANCE_CONTEXT_FIELDS``). Removals, retired grouping snapshots,
    orphaned result files and errors count; so do files kept because their
    source could not be verified and paths rejected as outside their root,
    which the Jobs summary warns about. A counter added later is kept by
    default instead of silently dropping the run.
    """
    if result.get("ok") is False or any(retired.values()):
        return False
    for key, value in result.items():
        if key in _MAINTENANCE_CONTEXT_FIELDS or isinstance(value, bool):
            continue
        if isinstance(value, (int, float, str, list, dict)) and value:
            return False
    return True


def utc_iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def metadata_repair_count(db: Database, workspace_id: int, root_paths: Sequence[str] | None = None) -> int:
    # Don't filter by ``folders.status``: that column is only refreshed
    # by ``folders.check_health`` (10-minute loop or the manual
    # "check missing folders" flow), so a workspace whose drive was
    # unplugged and is now reconnected still reads as ``status='missing'``
    # until then. The readiness endpoint fires as soon as the Import
    # page opens; if we filtered by ``status``, users would see 0
    # repairable photos and the repair route would 409 with "no
    # photos need metadata repair" even though ``os.path.isdir`` would
    # let the repair job scan them.
    #
    # ``root_paths`` scopes the count to real-time reachable roots
    # (based on ``os.path.isdir``). When a workspace mixes an offline
    # drive with photos missing EXIF and a separate reachable drive
    # with no repairable rows, an unscoped count combined with a
    # non-empty ``reachable_roots`` list would enable the Repair
    # button and start a job that finishes without ever touching the
    # offline photos — Codex's "repeating repair job" pathology. The
    # scoped count reflects only what the repair pass would actually
    # process. ``None`` preserves the unscoped legacy shape for any
    # future caller that wants a workspace-wide figure.
    rows = db.photo_visibility.metadata_repair_rows(workspace_id, root_paths)

    # A database row is only repairable when its original still exists.
    # The incremental repair scan discovers files from disk, so counting
    # a deleted/moved original here would leave the Repair button enabled
    # forever for a job that can never visit that row. Enumerate each
    # candidate folder once instead of statting every photo individually;
    # this keeps readiness responsive for large degraded imports.
    from image_loader import is_excluded_scan_path

    folder_files: dict[int, set[str] | None] = {}
    repairable = 0
    for row in rows:
        folder_id = row["folder_id"]
        folder_path = row["folder_path"]
        if folder_id not in folder_files:
            if is_excluded_scan_path(folder_path):
                folder_files[folder_id] = None
            else:
                try:
                    with os.scandir(folder_path) as entries:
                        folder_files[folder_id] = {
                            entry.name for entry in entries if entry.is_file()
                        }
                except OSError:
                    # The folder disappeared or became unreadable after
                    # root reachability was checked. Treat its rows as
                    # unavailable rather than offering a no-op repair.
                    folder_files[folder_id] = None

        names = folder_files[folder_id]
        if names is None:
            continue
        filename = row["filename"]
        if filename in names:
            repairable += 1
            continue
        # Preserve the filesystem's own case and Unicode matching rules
        # for a catalog name that did not compare byte-for-byte with the
        # directory entry (notably default APFS and NTFS volumes).
        if os.path.isfile(os.path.join(folder_path, filename)):
            repairable += 1
    return repairable
