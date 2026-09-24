"""App-construction and background maintenance passes.

``create_app`` runs a handful of self-healing passes around startup: the
ordered one-shot catalog repairs and config migrations
(``run_catalog_repairs``, including the synchronous species-marking pass that
gates the one-shot duplicate-species repair), the background species
mark/repair thread, the catalog-wide Wildlife genre retirement, the thumb_path
backfill job, and the teardown that stops the job runner and closes the
startup database. ``StartupTasks`` owns them because
they share per-app state: the startup taxonomy parse is cached on the instance
so overlapping passes do not each parse ``taxonomy.json``.

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

# Sentinel for "the startup taxonomy has not been parsed yet".
_TAXONOMY_NOT_LOADED = object()


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
        # Parsing taxonomy.json is expensive for a full iNaturalist download.
        # Cache the startup instance so overlapping one-time migrations and
        # the immediate background species pass do not each parse it
        # independently.
        self._startup_taxonomy = _TAXONOMY_NOT_LOADED

    def load_startup_taxonomy(self):
        # Do not cache a miss: a concurrent first-run taxonomy download can
        # make the file available before the background retry starts.
        if (
            self._startup_taxonomy is _TAXONOMY_NOT_LOADED
            or self._startup_taxonomy is None
        ):
            from taxonomy import load_local_taxonomy

            self._startup_taxonomy = load_local_taxonomy()
        return self._startup_taxonomy

    def run_catalog_repairs(self):
        """Run the ordered startup catalog repairs and config migrations.

        Every pass is idempotent and marker-gated (``db_meta`` or the config
        file's applied-migrations list), so later boots pay a few lookups.
        The order matters and is explained inline; ``create_app`` calls this
        once, on the startup connection, before registering any route.
        """
        import config as cfg

        init_db = self._init_db
        # File-backed startup skips Database's schema initialization. Repair old
        # move parentage here too, before Browse can serve the stale hierarchy.
        init_db.repair_stale_folder_parents()
        # Migrate the legacy 'Needs Classification' default collection BEFORE
        # seeding defaults — otherwise create_default_collections inserts
        # 'Needs Identification' first, then the migration skips renaming
        # because the target name already exists, leaving a duplicate.
        init_db.migrate_default_subject_collection()
        init_db.migrate_default_needs_identification_collection()
        init_db.migrate_default_location_collections()
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
        # Ungroup legacy bursts whose stored votes span more than one species.
        # Those rows display one species and, through accept_prediction's
        # vote-winner lookup, tag another; the repair makes them read as the
        # current classifier would have written them. Runs before the first
        # request so no page can render a row this is about to change, and
        # before any accept can act on one. Depends on no taxonomy or config,
        # so unlike the duplicate-species repair it needs no deferral: it is
        # db_meta-gated and self-logging, and later boots pay one marker
        # lookup. Method logs its own totals — the counts are the point, per
        # CORE_PHILOSOPHY.md.
        init_db.repair_mixed_species_prediction_groups()

        # Remove same-photo, same-taxon duplicate associations left by the old
        # hierarchy-import + top-level-confirmation interaction. Idempotent and
        # db_meta-gated, so later boots only pay for a single marker lookup.
        #
        # The repair identifies duplicates via
        # ``(is_species = 1 OR type = 'taxonomy') AND (rank = 'species' OR
        # taxon_id IS NULL)``. On upgraded databases a hierarchical species
        # leaf can still be a plain/general row until mark_species_keywords
        # retypes it, so run marking synchronously first — otherwise the
        # repair query cannot see the leaf, removes nothing, and still stamps
        # its one-shot marker; a subsequent background mark_species_keywords
        # pass could then make the leaf eligible while the redundant root
        # association remains permanently skipped. When taxonomy isn't
        # loaded yet (or marking fails), defer the repair to a later boot
        # rather than stamping the marker over an unmarked hierarchy.
        duplicate_repair_key = Database._DUPLICATE_PHOTO_SPECIES_REPAIR_KEY
        duplicate_repair_pending = init_db.get_meta(duplicate_repair_key) != "1"
        # The legacy bug always left a typed top-level species/taxonomy tag
        # beside another tag on the same photo. If no such pair exists, the
        # repair is structurally impossible and it is safe to stamp the
        # one-shot marker without parsing a potentially huge taxonomy file.
        if (
            duplicate_repair_pending
            and not init_db.has_possible_duplicate_photo_species()
        ):
            init_db.set_meta(duplicate_repair_key, "1")
            duplicate_repair_pending = False
            log.info(
                "Skipped duplicate-species startup repair: "
                "no possible duplicate associations"
            )

        if (
            duplicate_repair_pending
            and self.sync_mark_species_only(init_db, "sync-startup-species-mark")
        ):
            init_db.repair_duplicate_photo_species()
        # One-time rewrite of the previous miss-threshold defaults (0.25 / 0.15)
        # to the new defaults (0.20 / 0.12) in both ~/.vireo/config.json and
        # workspace overrides. Gated by a marker so it runs once; re-saved
        # legacy values are preserved on subsequent boots.
        cfg.migrate_legacy_miss_thresholds(init_db)
        # One-time rewrite of the previous eye-focus detection default from on to
        # off in both global config and workspace overrides.
        cfg.migrate_eye_detect_default_off(init_db)
        # One-time resolution of the browse.toggle_ui="h" default clashing with
        # any pre-existing browse binding on ``h``. Writes an explicit "" so the
        # user's existing action keeps working; they can re-bind toggle_ui from
        # the shortcuts editor.
        cfg.migrate_toggle_ui_h_conflict()
        # Existing users commonly have the previous Browse card defaults persisted
        # verbatim. Add the new coordinate-source field only for that exact legacy
        # list; customized card layouts remain unchanged.
        cfg.migrate_browse_location_status_field()
        # One-time rename of the "compare" navigation shortcut to "id_conflicts"
        # after the Compare page became ID Conflicts, so a user's saved binding
        # follows the page instead of being orphaned.
        cfg.migrate_compare_nav_id_to_id_conflicts()
        # One-time rewrite of the previous encounter-grouping species weight
        # (0.10) to the new default (0.40) in both ~/.vireo/config.json and
        # workspace overrides. Without this, upgraded installs that had the
        # pipeline block persisted verbatim keep grouping distinct species into
        # one encounter — the intended split behavior only reaches fresh
        # configs. Only the exact legacy value is rewritten; re-saved values
        # are preserved on subsequent boots.
        cfg.migrate_legacy_w_species_default(init_db)
        # One-time rewrite of the global pipeline.default_strategy (legacy
        # hardcoded strategy name) to pipeline.default_process_id (saved_processes
        # id). The workspace-side rewrite happens inside Database(); this covers
        # the global config file so workspaces that inherit the global default
        # don't silently fall back to import-only after upgrade.
        #
        # init_db uses initialize_schema=False for boot perf, so on the first
        # boot after upgrade the saved_processes table isn't guaranteed to
        # exist yet on this connection — the migration would silently defer
        # and any import in this session that would inherit the legacy global
        # default falls back to import-only until the *next* boot. Open a
        # short-lived schema-initializing handle so the migration completes on
        # the very first boot instead. Only pay the schema-init cost if the
        # migration hasn't been stamped yet; subsequent boots short-circuit
        # inside the function and pass ``init_db`` (whose schema state is
        # irrelevant because the marker check runs first).
        if (
            cfg.MIGRATION_DEFAULT_STRATEGY_TO_PROCESS_ID
            not in cfg._migrations_applied(cfg._read_raw())
        ):
            _default_strategy_migration_db = Database(self._db_path)
            try:
                cfg.migrate_default_strategy_to_process_id(
                    _default_strategy_migration_db,
                )
            finally:
                _default_strategy_migration_db.close()
        else:
            cfg.migrate_default_strategy_to_process_id(init_db)
        init_db.create_default_collections_for_all_workspaces()

    def sync_mark_species_only(self, db, log_label):
        """Load taxonomy and run mark_species_keywords synchronously.

        Returns True when the pass ran (marking either updated rows or
        found nothing to update), False when taxonomy is missing or the
        pass raised. Callers use the return value to gate follow-up work
        that depends on hierarchy leaves being correctly typed as
        taxonomy/is_species.
        """
        tax = self.load_startup_taxonomy()
        if tax is None:
            log.debug(
                "[%s] taxonomy not loaded; deferring species marking",
                log_label,
            )
            return False
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info(
                    "[%s] Marked %d keywords as species from taxonomy",
                    log_label, updated,
                )
            return True
        except Exception:
            log.debug(
                "[%s] mark_species_keywords failed",
                log_label, exc_info=True,
            )
            return False

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

    def mark_species_and_repair(self, db, log_label):
        """Load taxonomy, mark species keywords, and repair duplicates."""
        tax = self.load_startup_taxonomy()
        if tax is None:
            log.debug("[%s] taxonomy not loaded; deferring species marking", log_label)
            return
        try:
            updated = db.mark_species_keywords(tax)
            if updated:
                log.info("[%s] Marked %d keywords as species from taxonomy",
                         log_label, updated)
            repaired = db.repair_duplicate_photo_species()
            if repaired:
                log.info(
                    "[%s] Removed %d duplicate root species associations",
                    log_label, repaired,
                )
        except Exception:
            log.debug(
                "[%s] species marking/repair failed", log_label, exc_info=True,
            )

    def mark_species(self):
        """Background species mark/repair pass on its own connection."""
        bg_db = None
        try:
            bg_db = Database(self._db_path)
        except Exception:
            log.debug("Could not open background db for species marking", exc_info=True)
            return
        try:
            self.mark_species_and_repair(bg_db, "background")
        finally:
            bg_db.close()

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
        "JOIN workspace_folders wf ON wf.folder_id = f.id "
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
