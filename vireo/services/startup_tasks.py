"""App-construction and background maintenance passes.

``create_app`` runs a handful of self-healing passes around startup: the
ordered catalog repairs (``run_catalog_repairs``), the background thread that
imports keywords embedded in already-scanned files and then marks species,
the catalog-wide Wildlife genre retirement, the thumb_path and label-identity
backfill jobs, and the teardown that stops the job runner and closes the
startup database. ``StartupTasks`` owns them because they share per-app
state (the app, its database path and the startup connection).

This module decides *what* each pass does. *When* it runs (the
``VIREO_DISABLE_STARTUP_BACKFILL_TIMERS`` guards, thread and timer start
points, delays and daemon flags) stays in ``create_app`` so startup ordering is
readable in one place.

``metadata_repair_count`` (the Import page's repair readiness count) and
``utc_iso_now`` are plain functions with no per-app state.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

from db import Database

log = logging.getLogger(__name__)

class StartupTasks:
    """Startup and maintenance passes bound to one app.

    ``app`` is read at call time (``app.config["THUMB_CACHE_DIR"]``,
    ``app._job_runner``, ``app._log_broadcaster``), so the instance can be
    built before those attributes are set. ``init_db`` is the startup
    connection ``create_app`` opened; ``db_path`` is used to open the
    short-lived connections background passes need.
    """

    def __init__(self, app, db_path, init_db):
        self._app = app
        self._db_path = db_path
        self._init_db = init_db

    def run_catalog_repairs(self):
        """Run the ordered startup catalog repairs.

        Each pass is idempotent and cheap when there is nothing to repair.
        ``create_app`` calls this once, on the startup connection, before
        registering any route.
        """
        init_db = self._init_db
        # File-backed startup skips Database's schema initialization. Repair old
        # move parentage here too, before Browse can serve the stale hierarchy.
        init_db.repair_stale_folder_parents()
        # One-shot keyword-name normalization backfill. Database.__init__ only
        # runs it when initialize_schema=True, and every file-backed connection
        # this app opens — including this startup init_db and every per-request
        # connection at `_get_db` — passes initialize_schema=False. Without an
        # explicit run here, an upgraded DB can serve requests with `‘apapane`-
        # style variant rows still present until some background job happens
        # to construct a full `Database()` (initialize_schema=True); in that
        # window an add/rename can miss the legacy row and create duplicate
        # tags or stale XMP. The method is idempotent (db_meta-gated) so
        # subsequent boots are a cheap SELECT.
        init_db.normalize_keyword_data()
        repaired_location_ancestors = init_db.repair_misclassified_location_ancestors()
        if repaired_location_ancestors:
            log.info(
                "Restored %d location hierarchy nodes misclassified as taxonomy",
                repaired_location_ancestors,
            )
        init_db.create_default_collections_for_all_workspaces()

    def retire_wildlife_genre(self):
        """Run the catalog-wide XMP migration outside startup readiness.

        Large upgraded catalogs can require tens of thousands of sidecar
        reads here. Keeping that work on create_app's calling thread prevents
        the HTTP listener from binding and makes the desktop launcher report
        a false startup failure when its readiness deadline expires.
        """
        retirement_db = None
        started_at = time.time()
        try:
            retirement_db = Database(self._db_path)
            retired = retirement_db.retire_builtin_wildlife_genre()
            if retired:
                log.info(
                    "Retired the built-in Wildlife genre from %d photo(s)",
                    retired,
                )
            log.info(
                "Wildlife genre retirement finished in %.2fs",
                time.time() - started_at,
            )
            return retired
        except Exception:
            log.exception("Wildlife genre retirement failed")
            return 0
        finally:
            if retirement_db is not None:
                retirement_db.close()

    def mark_species_keywords(self, db, log_label):
        """Load taxonomy and type keywords that name a species."""
        from taxonomy import load_local_taxonomy

        tax = load_local_taxonomy()
        if tax is None:
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
        except Exception:
            log.debug(
                "[%s] species marking failed", log_label, exc_info=True,
            )

    def mark_species(self):
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

    def backfill_embedded_keywords(self, db):
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
            db.conn.rollback()
            return 0
        if imported:
            log.info(
                "Imported embedded keywords onto %d photos in %.2fs",
                imported, time.time() - started_at,
            )
        return imported

    def cleanup_app_resources(self, job_timeout=10.0):
        """Stop background jobs, uninstall the log broadcaster, close init_db.

        Returns whether the job runner shut down within ``job_timeout``.
        """
        app = self._app
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

    def kickoff_thumb_path_backfill(self):
        """Start the ephemeral thumb_path backfill job when it has work.

        The dashboard's coverage card counts thumbnails by ``thumb_path IS
        NOT NULL``, but for a long stretch the column was never populated by
        production code, so libraries with 40k JPEGs cached on disk reported
        "0 thumbnails" forever. This pass aligns the column with disk reality
        for legacy rows, and clears it for photos whose cached file has since
        been deleted (drift correction). It runs as an ephemeral JobRunner job
        (so it shows in the bottom panel), is never written to job_history,
        and is skipped entirely when a fast count check finds nothing to do.
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

        def work(job):
            thread_db = Database(db_path)
            try:
                active_ws = init_db._active_workspace_id
                if active_ws is not None:
                    thread_db.set_active_workspace(active_ws)

                def progress_cb(current, total):
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

                def status_cb(message, **_phase):
                    runner.push_event(job["id"], "progress", {
                        "phase": message,
                        "current": job["progress"].get("current", 0),
                        "total": job["progress"].get("total", 0),
                    })

                def cancel_check():
                    return runner.is_cancelled(job["id"])

                return backfill_thumb_paths(
                    thread_db, cache_dir,
                    progress_callback=progress_cb,
                    status_callback=status_cb,
                    cancel_check=cancel_check,
                )
            finally:
                thread_db.close()

        try:
            runner.start(
                "thumb_path_backfill", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start thumb_path backfill job")

    def kickoff_label_identity_backfill(self):
        """Recover taxon IDs for predictions made from legacy label lists.

        Lists saved before they recorded identities leave BioCLIP predictions
        identified by name only, so a species whose name the taxonomy calls
        ambiguous shows up twice in review. See ``label_source_identities``.
        The pass re-queries iNaturalist, so it runs as an ephemeral JobRunner
        job (visible in the bottom panel) and only when a label set needs it;
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

        def work(job):
            thread_db = Database(db_path)
            try:
                def progress(current, total, phase):
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    runner.push_event(job["id"], "progress", {
                        "current": current, "total": total, "phase": phase,
                    })

                return backfill(
                    thread_db, progress=progress,
                    cancel_check=lambda: runner.is_cancelled(job["id"]),
                )
            finally:
                thread_db.close()

        try:
            runner.start(
                "label-list-species-ids", work,
                ephemeral=True,
                config={"trigger": "startup"},
            )
        except Exception:
            log.exception("Failed to start label list species ID job")


def utc_iso_now():
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def metadata_repair_count(db, workspace_id, root_paths=None):
    # Don't filter by ``folders.status``: that column is only refreshed
    # by ``check_folder_health`` (10-minute loop or the manual
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
    params = [workspace_id]
    where_extra = ""
    if root_paths is not None:
        if not root_paths:
            return 0
        normalized_roots = [
            r.replace("\\", "/").rstrip("/") for r in root_paths if r
        ]
        if not normalized_roots:
            return 0
        clauses = []
        for norm in normalized_roots:
            prefix = norm + "/"
            # Match ``f.path`` normalized to forward slashes either
            # exactly against the root or as a boundary-preserving
            # prefix. ``substr(...)=prefix`` avoids the wildcard
            # collision LIKE would introduce (e.g. a folder called
            # ``photos_backup`` incorrectly matching a reachable
            # ``photos`` root because ``_`` matches any character
            # in LIKE without ESCAPE).
            clauses.append(
                "(REPLACE(f.path, '\\', '/') = ? "
                "OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?)"
            )
            params.extend([norm, len(prefix), prefix])
        where_extra = " AND (" + " OR ".join(clauses) + ")"
    rows = db.conn.execute(
        "SELECT DISTINCT p.id, p.filename, "
        "f.id AS folder_id, f.path AS folder_path "
        "FROM photos p "
        "JOIN folders f ON f.id = p.folder_id "
        "JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
        "WHERE wf.workspace_id = ? "
        "AND p.exif_data IS NULL"
        + where_extra
        + " ORDER BY f.path, p.filename",
        params,
    ).fetchall()

    # A database row is only repairable when its original still exists.
    # The incremental repair scan discovers files from disk, so counting
    # a deleted/moved original here would leave the Repair button enabled
    # forever for a job that can never visit that row. Enumerate each
    # candidate folder once instead of statting every photo individually;
    # this keeps readiness responsive for large degraded imports.
    from image_loader import is_excluded_scan_path

    folder_files = {}
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
