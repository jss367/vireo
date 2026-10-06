"""Admission and background workflow for in-place imports.

``enqueue_import_in_place`` validates the request and registers the job.
``_InPlaceImportJob`` holds what the request admitted, and each execution
of the job keeps its progress, scan scope and outcomes on an
``_InPlaceImportRun``.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from db import Database
from metadata import scan_metadata_warning
from new_images import invalidate_new_images_after_scan
from services.imports import ImportFailure
from services.local_folder import local_copy_scan_conflict, stage_pending_source_paths
from services.local_workspace import stage_boundary_lock

if TYPE_CHECKING:
    from services.imports import ImportService

log = logging.getLogger(__name__)


def enqueue_import_in_place(service: ImportService, db: Database, body: dict) -> dict | ImportFailure:
    """Import existing folders or a new-images snapshot without copying.

    This is the in-place companion to ``/api/jobs/import-photos``: scan
    selected source folders into the active workspace, leave originals at
    their current paths, and optionally enqueue the same after-import
    processing strategy used by archive-copy imports. Snapshot mode is the
    catalog-admission boundary for files discovered below registered roots:
    it scans only the frozen paths and never promotes their leaf folders to
    additional workspace roots.
    """
    from image_loader import is_excluded_scan_path

    source_snapshot_id = body.get("source_snapshot_id")
    copy_only_error = _copy_only_option_error(body)
    if copy_only_error is not None:
        return copy_only_error
    dependency_error = service._validate_import_metadata_dependency(body)
    if dependency_error is not None:
        return dependency_error
    sources = body.get("sources")
    if isinstance(sources, str):
        sources = [sources]
    snapshot_paths = None

    if source_snapshot_id is not None:
        snapshot_request_error = _snapshot_request_error(
            body, sources, source_snapshot_id,
        )
        if snapshot_request_error is not None:
            return snapshot_request_error
    else:
        sources_error = _explicit_sources_error(
            service, db, sources, is_excluded_scan_path,
        )
        if sources_error is not None:
            return sources_error
        # Re-checked atomically with ``runner.start`` below so a stage
        # request that races between the pre-flight release and job
        # registration still blocks on the same boundary lock.
        _conflict_paths = list(sources)

    recursive = bool(body.get("recursive", True))
    import_tags, location_from_gps, tag_options_err = (
        service._validate_import_tag_options(body)
    )
    if tag_options_err is not None:
        return tag_options_err
    if source_snapshot_id is not None:
        snapshot_paths, snapshot_paths_by_root, snapshot_err = (
            _resolve_snapshot_paths(service, db, source_snapshot_id)
        )
        if snapshot_err is not None:
            return snapshot_err
        sources = sorted(snapshot_paths_by_root)
        # Re-checked atomically with ``runner.start`` below. See the
        # explicit-sources branch above for why the pre-flight check is
        # not sufficient on its own.
        _conflict_paths = list(snapshot_paths)
    else:
        snapshot_paths_by_root = None
    # Preflight an explicit after_import before creating a workspace so
    # a bad value doesn't leave an orphan Card Import behind. The
    # omitted branch has to wait until AFTER the workspace switch — see
    # below.
    explicit_after_import = "after_import" in body
    after_import = None
    if explicit_after_import:
        after_import = body.get("after_import")
        err = service._validate_after_import(after_import, db)
        if err is not None:
            return err

    active_ws, created_workspace, previous_active_ws, workspace_err = (
        service._prepare_import_workspace(db, body)
    )
    if workspace_err is not None:
        return workspace_err

    # Everything from here to ``runner.start`` runs inside
    # ``_admit_into_import_workspace``: a ``new_workspace_name`` request
    # has already committed the workspace and switched active to it, so
    # any failure, returned or raised, must undo both. Add new checks
    # before ``_prepare_import_workspace`` or inside ``_admit_in_place_job``.
    def admit():
        return _admit_in_place_job(
            service, db, body,
            active_ws=active_ws,
            created_workspace=created_workspace,
            explicit_after_import=explicit_after_import,
            after_import=after_import,
            sources=sources,
            snapshot_paths=snapshot_paths,
            snapshot_paths_by_root=snapshot_paths_by_root,
            source_snapshot_id=source_snapshot_id,
            recursive=recursive,
            import_tags=import_tags,
            location_from_gps=location_from_gps,
            conflict_paths=_conflict_paths,
        )

    return service._admit_into_import_workspace(
        db, created_workspace, previous_active_ws, admit,
    )


def _admit_in_place_job(
    service, db, body, *, active_ws, created_workspace,
    explicit_after_import, after_import, sources, snapshot_paths,
    snapshot_paths_by_root, source_snapshot_id, recursive, import_tags,
    location_from_gps, conflict_paths,
):
    """The admission steps after the workspace switch; returns a job id or
    an ``ImportFailure``. ``runner.start`` must stay the last step."""
    # Resolve the omitted-default AFTER the workspace switch. Reading
    # pipeline.default_process_id off the previously-active workspace
    # would leak that workspace's override into a new-workspace import.
    if not explicit_after_import:
        after_import, err = _default_after_import(service, db)
        if err is not None:
            return err

    after_import_snapshot, err = _after_import_process_snapshot(
        db, after_import,
    )
    if err is not None:
        return err

    runner = service.get_runner()
    thumb_cache_dir = service.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_cache_dir)
    snapshot_import_lock = None
    if source_snapshot_id is not None:
        snapshot_import_lock = _snapshot_import_lock(
            service, active_ws, source_snapshot_id,
        )

    import_job = _InPlaceImportJob(
        service=service,
        runner=runner,
        active_ws=active_ws,
        sources=sources,
        snapshot_paths=snapshot_paths,
        snapshot_paths_by_root=snapshot_paths_by_root,
        source_snapshot_id=source_snapshot_id,
        recursive=recursive,
        import_tags=import_tags,
        location_from_gps=location_from_gps,
        after_import=after_import,
        after_import_snapshot=after_import_snapshot,
        thumb_cache_dir=thumb_cache_dir,
        vireo_dir=vireo_dir,
        snapshot_import_lock=snapshot_import_lock,
    )
    job_config = import_job.job_config(body, created_workspace)
    # Snapshot-backed imports serialize on ``snapshot_import_lock`` for
    # the entire worker call. Pausing inside that critical section would
    # sleep while holding the shared lock, so a queued second import for
    # the same snapshot would block on the bare lock acquisition and
    # could not reach any runner checkpoint — cancelling it would have
    # no effect until the first import resumed. Keep this mode
    # non-pausable while the lock is in play; other in-place imports
    # remain pausable.
    #
    # Re-check ``local_copy_scan_conflict`` and register the runner job
    # atomically under ``stage_boundary_lock``. A folder-stage request
    # that arrives after the earlier pre-flight release but before this
    # registration would otherwise see no import job and be admitted,
    # letting the stage rebase paths this import is about to walk.
    with stage_boundary_lock():
        conflict = _local_copy_conflict(db, runner, conflict_paths)
        if conflict:
            return ImportFailure(conflict, 409)
        return runner.start(
            "import-in-place", import_job.work, config=job_config,
            workspace_id=active_ws,
            pausable=snapshot_import_lock is None,
        )


def _default_after_import(service, db):
    """The active workspace's default after-import process, validated."""
    import config as cfg

    effective_cfg = db.get_effective_config(cfg.load())
    after_import = (
        effective_cfg.get("pipeline", {}).get("default_process_id")
    )
    err = service._validate_after_import(after_import, db)
    if err is not None:
        return None, err
    return after_import, None


def _after_import_process_snapshot(db, after_import):
    """Snapshot the chosen saved process's stage flags at enqueue time.

    A mid-import edit or delete then can't silently change (or void)
    the after-import run the user already accepted. An in-place
    import can take many minutes on a full card, and until the
    chain hook fires the pipeline_job's actual toggles are still up
    for grabs — resolving here freezes them.
    """
    after_import_snapshot = None
    if after_import is not None:
        try:
            after_import_snapshot = db.resolve_process(after_import)
        except ValueError as e:
            return None, ImportFailure(str(e), 404)
    return after_import_snapshot, None


def _snapshot_import_lock(service, active_ws, source_snapshot_id):
    snapshot_lock_key = (active_ws, source_snapshot_id)
    with service.snapshot_import_locks_guard:
        return service.snapshot_import_locks.setdefault(
            snapshot_lock_key, threading.Lock(),
        )


def _copy_only_option_error(body):
    """Reject the options that only make sense when copying to an archive."""
    if body.get("local_processing"):
        return ImportFailure("Local processing with temporary storage requires Copy to archive")
    if body.get("defer_nas_transfer"):
        return ImportFailure("Keeping photos local before NAS transfer requires Copy to archive")
    if body.get("after_process_move") is not None:
        return ImportFailure(
            "after_process_move is not supported for import-in-place — "
            "photos stay where they are; use Copy to archive"
        )
    return None


def _snapshot_request_error(body, sources, source_snapshot_id):
    """Reject a snapshot request that also names sources or a new workspace."""
    if sources:
        return ImportFailure(
            "source_snapshot_id cannot be combined with sources"
        )
    if "new_workspace_name" in body:
        return ImportFailure(
            "a new-images snapshot belongs to the active workspace "
            "and cannot be imported into a new workspace"
        )
    if (
        isinstance(source_snapshot_id, bool)
        or not isinstance(source_snapshot_id, int)
    ):
        return ImportFailure("source_snapshot_id must be an integer")
    return None


def _explicit_sources_error(service, db, sources, is_excluded_scan_path):
    """Validate explicit source folders and pre-flight the local-copy check."""
    if not sources or not isinstance(sources, list) or not all(
        isinstance(s, str) and s for s in sources
    ):
        return ImportFailure("sources must be a non-empty list of paths")
    for s in sources:
        if is_excluded_scan_path(s):
            return ImportFailure(
                f"source is inside a macOS app-managed library and "
                f"cannot be imported: {s}"
            )
        if not os.path.isdir(s):
            return ImportFailure(f"source directory not found: {s}")
    return _preflight_local_copy_conflict(service, db, sources)


def _local_copy_conflict(db, runner, paths):
    """The local-copy conflict for ``paths``; the caller holds ``stage_boundary_lock``."""
    pending_sources = stage_pending_source_paths(
        runner.list_jobs
        if runner is not None else None,
        db,
    )
    return local_copy_scan_conflict(
        db, paths,
        active_workspace_id=db._active_workspace_id,
        pending_stage_sources=pending_sources,
    )


def _preflight_local_copy_conflict(service, db, paths):
    runner = service.get_runner()
    with stage_boundary_lock():
        conflict = _local_copy_conflict(db, runner, paths)
    if conflict:
        return ImportFailure(conflict, 409)
    return None


def _resolve_snapshot_paths(service, db, source_snapshot_id):
    """The snapshot's frozen paths, grouped by the registered root holding each.

    Returns ``(snapshot_paths, snapshot_paths_by_root, error)``.
    """
    snap = db.get_new_images_snapshot(source_snapshot_id)
    if snap is None:
        return None, None, ImportFailure(
            f"source_snapshot_id {source_snapshot_id} not found",
            status=404,
        )
    snapshot_paths = list(snap["file_paths"])

    # Resolve each frozen path to one of the workspace's registered
    # roots. The snapshot was created from these roots, but validate
    # again at enqueue time so a stale/crafted snapshot can never use
    # Import as a path-admission escape hatch after workspace roots
    # change.
    from new_images import mapped_roots as _mapped_new_image_roots

    registered_roots = sorted(
        (
            (os.path.normpath(r["path"]), r["path"])
            for r in _mapped_new_image_roots(
                db, db._active_workspace_id, include_missing=True,
            )
        ),
        key=lambda item: len(item[0]),
        reverse=True,
    )

    snapshot_paths_by_root = {}
    for path in snapshot_paths:
        root = _registered_root_for(path, registered_roots)
        if root is None:
            return None, None, ImportFailure(
                "new-images snapshot contains a path outside the "
                f"active workspace's registered folders: {path}"
            )
        snapshot_paths_by_root.setdefault(root, []).append(path)
    # A snapshot can have been captured before a descendant of one of
    # its roots was staged as a local copy by another workspace. The
    # worker restricts the scan to snapshot_paths but the scanner
    # canonicalizes folder paths via realpath, so a frozen file inside
    # a staged source would still be catalogued a second time at its
    # original path. Refuse the whole import if any snapshot path
    # falls within (or is aliased to) a staged source; the user syncs
    # or discards the local copy first.
    conflict_error = _preflight_local_copy_conflict(
        service, db, snapshot_paths,
    )
    if conflict_error is not None:
        return None, None, conflict_error
    return snapshot_paths, snapshot_paths_by_root, None


def _registered_root_for(path, registered_roots):
    candidate = os.path.normpath(path)
    for normalized_root, stored_root in registered_roots:
        try:
            if (
                os.path.commonpath([candidate, normalized_root])
                == normalized_root
            ):
                # Containment uses normalized paths, but scanner's
                # parent walk is lexical. Preserve the spelling
                # that produced the frozen snapshot paths so a
                # registered root containing ".." still meets its
                # restricted descendants exactly.
                return stored_root
        except ValueError:
            continue
    return None


@dataclass
class _InPlaceImportJob:
    """What the request admitted, fixed when the job is enqueued."""

    service: ImportService
    runner: object
    active_ws: int
    sources: list
    snapshot_paths: list | None
    snapshot_paths_by_root: dict | None
    source_snapshot_id: int | None
    recursive: bool
    import_tags: object
    location_from_gps: object
    after_import: object
    after_import_snapshot: object
    thumb_cache_dir: str
    vireo_dir: str
    snapshot_import_lock: object

    def job_config(self, body, created_workspace):
        return {
            "sources": self.sources,
            "source_snapshot_id": self.source_snapshot_id,
            "destination": None,
            "recursive": self.recursive,
            "after_import": self.after_import,
            "tags": self.import_tags,
            "location_from_gps": self.location_from_gps,
            "allow_missing_exiftool": bool(
                body.get("allow_missing_exiftool", False)
            ),
            "mode": "in_place",
            "workspace_id": self.active_ws,
            "created_workspace": created_workspace,
        }

    def work(self, job):
        if self.snapshot_import_lock is None:
            return self._run_import_in_place(job)
        with self.snapshot_import_lock:
            return self._run_import_in_place(job)

    def _run_import_in_place(self, job):
        # Scan callbacks form reference cycles that can outlive the job.
        # Release SQLite files on every return/error without waiting for GC.
        with Database(self.service.db_path) as thread_db:
            return _InPlaceImportRun(self, job, thread_db).run()

    def _chain_after_import(self, job, result):
        service = self.service
        after_import = self.after_import
        photo_ids = result.get("photo_ids") or []

        # A frozen snapshot can still yield existing IDs through the
        # scanner callback when it is replayed. Those IDs make tagging
        # idempotent, but they must not create another import collection
        # or enqueue an expensive Process run: this Import admitted
        # nothing new.
        if (
            result.get("ok")
            and result.get("source_snapshot_id") is not None
            and result.get("imported") == 0
        ):
            result["after_import_skipped"] = (
                "import-only" if after_import is None else "no new photos"
            )
            return

        carry_photo_ids = list(
            (job.get("config") or {}).get("carry_photo_ids") or []
        )
        thread_db, col_id = service._record_import_collection(
            result, self.active_ws, chain_photo_ids=carry_photo_ids,
        )
        chain_scope = photo_ids + list(
            result.get("carried_photo_ids") or []
        )

        if after_import is None:
            result["after_import_skipped"] = "import-only"
            return
        if not result.get("ok"):
            result["after_import_skipped"] = "import failed"
            return
        if result.get("cancelled"):
            result["after_import_skipped"] = "import cancelled"
            return
        if not chain_scope:
            result["after_import_skipped"] = "no photos"
            return
        if col_id is None:
            result["after_import_skipped"] = (
                "failed to create import collection"
            )
            return
        try:
            process_job_id, model_warning, process_blocker = service.enqueue_process_job(
                thread_db, self.runner, self.active_ws,
                collection_id=col_id,
                process_id=after_import,
                chained_from=job["id"],
                expanded=self.after_import_snapshot,
            )
            if process_blocker:
                result["after_import_skipped"] = process_blocker
                return
            result["process_job_id"] = process_job_id
            if model_warning:
                result["model_warning"] = model_warning
        except Exception as e:
            log.exception("after-import chaining failed")
            result["after_import_skipped"] = (
                f"failed to enqueue processing: {e}"
            )


@dataclass
class _SourceScope:
    """The snapshot restriction one source's scan replays."""

    files: set | None = None
    dirs: list | None = None
    dir_identities: dict = field(default_factory=dict)


class _InPlaceImportRun:
    """State shared by every phase of one in-place import job run."""

    def __init__(self, plan, job, thread_db):
        self.plan = plan
        self.job = job
        self.thread_db = thread_db
        self.runner = plan.runner
        self.sources = plan.sources
        self.snapshot_paths = plan.snapshot_paths
        self.snapshot_paths_by_root = plan.snapshot_paths_by_root

        self.photo_ids = []
        self.seen_photo_ids = set()
        self.indexed_paths = set()
        self.root_errors = []
        self.scan_acc = {
            "prior": 0,
            "last_current": 0,
            "last_total": 0,
            "overall_total": 0,
            "source_index": 0,
        }
        self.working_copy_scope = []
        self.working_copy_scope_baselines = {}
        self.working_copy_scope_identities = {}
        self.source_mount_baselines = {}
        self.source_mount_identities = {}

        self.snapshot_requested = len(self.snapshot_paths or [])
        self.snapshot_missing = []
        self.snapshot_unreadable = []
        self.snapshot_eligible = set(self.snapshot_paths or [])
        self.snapshot_known_before = {}

        self.source_manifests = {}
        # Per-source dict of directory-path → mount identity captured at
        # discovery time. Discovery walks each source and produces a
        # frozen list of files, but only the source root's identity is
        # baselined earlier (source_mount_identities). Later sources can
        # wait minutes behind earlier ones, and in that window a nested
        # child dir or symlink under an already-discovered source may be
        # swapped for an ordinary photo subtree with the same name — a
        # replacement is_excluded_scan_path cannot recognize and the
        # source-root check does not see. Recording an identity per
        # unique parent directory of the manifest lets the scan loop
        # reject the source before scanner.stat()s the frozen filenames
        # under the replacement.
        self.source_manifest_dir_identities = {}
        self.source_discovery_failures = set()
        self.cancelled = False

    # -- the run -----------------------------------------------------

    def run(self):
        import errno as errno_mod

        import config as cfg
        from ingest import discover_source_files
        from pipeline_job import (
            _archive_mount_baseline,
            _changed_mount_since_baseline,
            _load_known_mount_roots,
            _mount_identity,
            _mount_identity_baseline,
            _record_known_mount_roots,
            _unmounted_since_baseline,
        )
        from scanner import (
            ScanCancelled,
            _extract_working_copies,
            is_excluded_scan_path,
        )
        from scanner import (
            scan as do_scan,
        )

        self.errno_mod = errno_mod
        self.discover_source_files = discover_source_files
        self.archive_mount_baseline = _archive_mount_baseline
        self.changed_mount_since_baseline = _changed_mount_since_baseline
        self.mount_identity = _mount_identity
        self.mount_identity_baseline = _mount_identity_baseline
        self.record_known_mount_roots = _record_known_mount_roots
        self.unmounted_since_baseline = _unmounted_since_baseline
        self.scan_cancelled = ScanCancelled
        self.extract_working_copies = _extract_working_copies
        self.is_excluded_scan_path = is_excluded_scan_path
        self.do_scan = do_scan

        self._start(cfg)
        self._baseline_source_mounts(_load_known_mount_roots)
        if self.snapshot_paths is not None:
            self._freeze_snapshot_outcomes()
        self._discover_sources()
        self._publish_overall_total()
        self._scan_sources()
        if not self.cancelled and self.working_copy_scope:
            self._extract_deferred_working_copies()
        return self._finish()

    def _start(self, cfg):
        thread_db = self.thread_db
        thread_db.set_active_workspace(self.plan.active_ws)
        if thread_db.check_folder_health():
            self.plan.service.invalidate_missing_originals()
        effective_cfg = thread_db.get_effective_config(cfg.load())
        self.pipeline_cfg = effective_cfg.get("pipeline", {})

        self.job["_start_time"] = time.time()
        self.runner.set_steps(self.job["id"], [
            {"id": "scan", "label": "Import in place"},
        ])
        self.runner.update_step(self.job["id"], "scan", status="running")

    def _baseline_source_mounts(self, load_known_mount_roots):
        known_mount_roots = load_known_mount_roots(self.thread_db)
        for source in self.sources:
            source_key = str(Path(source))
            baseline = self.archive_mount_baseline(
                source, known_mount_roots,
            )
            self.source_mount_baselines[source_key] = baseline
            identities = self.mount_identity_baseline(baseline)
            # Mount roots catch detach/remount; the source directory's
            # own inode also catches a local root renamed and replaced
            # while later sources are still scanning.
            identities[source_key] = self.mount_identity(source_key)
            self.source_mount_identities[source_key] = identities
            self.record_known_mount_roots(self.thread_db, baseline)

    def _active_photo_ids_by_path(self):
        """Map primary and companion paths to active photo records."""
        rows = self.thread_db.conn.execute(
            """SELECT p.id, p.filename, p.companion_path,
                      f.path AS folder_path
               FROM photos p
               JOIN folders f ON f.id = p.folder_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?""",
            (self.plan.active_ws,),
        ).fetchall()
        result = {}
        for row in rows:
            result[
                os.path.join(row["folder_path"], row["filename"])
            ] = row["id"]
            if row["companion_path"]:
                companion_path = row["companion_path"]
                if not os.path.isabs(companion_path):
                    companion_path = os.path.join(
                        row["folder_path"], companion_path,
                    )
                result[companion_path] = row["id"]
        return result

    def _freeze_snapshot_outcomes(self):
        # Freeze execution outcomes before scanning. Missing and
        # unreadable files stay visible in the result instead of
        # silently shrinking the banner's promised count.
        self.snapshot_eligible.clear()
        for path in self.snapshot_paths:
            if not os.path.isfile(path):
                self.snapshot_missing.append(path)
                continue
            try:
                with open(path, "rb") as fh:
                    fh.read(1)
            except OSError:
                self.snapshot_unreadable.append(path)
                continue
            self.snapshot_eligible.add(path)

        # Record active-workspace membership before the scan so a
        # concurrent/repeated import is reported as idempotent rather
        # than as a newly admitted photo.
        self.snapshot_known_before = {
            path: photo_id
            for path, photo_id in self._active_photo_ids_by_path().items()
            if path in self.snapshot_eligible
        }

    def _record_error(self, msg):
        self.root_errors.append(msg)
        if msg not in self.job["errors"]:
            self.job["errors"].append(msg)

    # -- scanner callbacks -------------------------------------------

    def _photo_cb(self, photo_id, path):
        if photo_id not in self.seen_photo_ids:
            self.seen_photo_ids.add(photo_id)
            self.photo_ids.append(photo_id)
        self.indexed_paths.add(path)
        self.runner.update_step(
            self.job["id"], "scan", current_file=os.path.basename(path),
        )

    def _photo_merged_cb(self, old_id, new_id, path):
        """Follow a JPEG photo the scan's pairing pass merged into its RAW.

        ``old_id`` no longer names a photo and could be given to the next
        insert, so the import collection, tags and chained Process job take
        the RAW instead.
        """
        if old_id not in self.seen_photo_ids:
            return
        self.seen_photo_ids.discard(old_id)
        self.photo_ids = [pid for pid in self.photo_ids if pid != old_id]
        if new_id is not None:
            self._photo_cb(new_id, path)

    def _progress_cb(self, current, total):
        job = self.job
        scan_acc = self.scan_acc
        scan_acc["last_current"] = current
        scan_acc["last_total"] = total
        cum_current = scan_acc["prior"] + current
        cum_total = scan_acc["overall_total"]
        job["progress"]["current"] = cum_current
        job["progress"]["total"] = cum_total
        self.runner.update_step(
            job["id"], "scan",
            progress={"current": cum_current, "total": cum_total},
        )
        self.runner.push_event(job["id"], "progress", {
            "current": cum_current,
            "total": cum_total,
            "current_file": job["progress"].get("current_file", ""),
            "phase": "Importing in place",
            # Explicitly clear a completed discovery/metadata phase.
            # JobRunner mirrors progress by merging keys, so omitting
            # these would leave the old phase active for poll clients.
            "phase_current": None,
            "phase_total": None,
            "phase_label": None,
        })

    def _status_cb(self, message, phase_current=None, phase_total=None, phase_label=None):
        job = self.job
        sources = self.sources
        visible_phase_label = phase_label
        if (
            phase_label
            and phase_label != "Generating working copies"
            and len(sources) > 1
        ):
            visible_phase_label = (
                f"{phase_label} — source "
                f"{self.scan_acc['source_index']} of {len(sources)}"
            )
        job["progress"]["current_file"] = message
        step_update = {"current_file": message}
        if (
            phase_total
            and phase_label == "Generating working copies"
        ):
            # The scan counter can already be complete while working
            # copies are still being generated. Put the active phase
            # on the expanded Jobs-page step too, rather than leaving
            # that row pinned at a misleading 100%.
            step_update["progress"] = {
                "current": phase_current or 0,
                "total": phase_total,
            }
        self.runner.update_step(job["id"], "scan", **step_update)
        self.runner.push_event(job["id"], "progress", {
            "current": job["progress"].get("current", 0),
            "total": job["progress"].get("total", 0),
            "current_file": message,
            "phase": visible_phase_label or message,
            "phase_current": phase_current,
            "phase_total": phase_total,
            "phase_label": visible_phase_label,
        })

    def _advance_scan_acc(self):
        self.scan_acc["prior"] += self.scan_acc["last_current"]
        self.scan_acc["last_current"] = 0
        self.scan_acc["last_total"] = 0

    def _cancel_check(self):
        return self.runner.is_cancelled(self.job["id"])

    def _pause_check(self):
        return self.runner.pause_requested(self.job["id"])

    def _cancel_only_check(self):
        return self.runner.cancellation_requested(self.job["id"])

    # -- discovery ---------------------------------------------------

    def _emit_discovery(self, source_index, source, checked=0, found=0):
        sources = self.sources
        source_name = os.path.basename(os.path.normpath(source)) or source
        message = (
            f"Discovering source {source_index} of {len(sources)}: "
            f"{source_name}"
        )
        if checked:
            message += f" ({found:,} files found)"
        self.runner.update_step(self.job["id"], "scan", current_file=message)
        self.runner.push_event(self.job["id"], "progress", {
            "current": 0,
            "total": 0,
            "current_file": message,
            "phase": message,
            "phase_current": source_index - 1,
            "phase_total": len(sources),
            "phase_label": "Discovering sources",
        })

    def _discover_sources(self):
        """Discover every source before processing any of them.

        Previously each scan discovered its source just-in-time, so the UI
        called a partial denominator "Overall" and then moved backward when
        the next source added more files. Freezing these manifests makes the
        total stable and also excludes files that arrive mid-import.
        """
        sources = self.sources
        for source_index, source in enumerate(sources, 1):
            if self._cancel_check():
                self.cancelled = True
                break
            self._emit_discovery(source_index, source)
            try:
                manifest = self._discover_manifest(source_index, source)
            except self.scan_cancelled:
                self.cancelled = True
                break
            except Exception as exc:
                log.exception(
                    "In-place import discovery failed for source %s", source,
                )
                self._record_error(f"[{source}] discovery failed: {exc}")
                self.source_discovery_failures.add(source)
                manifest = []
            self.source_manifests[source] = manifest
            # Baseline the identity of each unique parent directory in
            # the manifest. Deduplicating first bounds this to the tree
            # depth actually observed, not the file count. Only applied
            # to sources whose manifest came from a filesystem walk here:
            # snapshot-mode manifests take an explicit path list captured
            # earlier by the caller, and their per-directory identity is
            # already captured just before scan by restricted_dir_identities
            # and rechecked post-scan when scoping working-copy extraction.
            # See source_manifest_dir_identities for the full rationale.
            if self.snapshot_paths_by_root is None:
                manifest_dir_identities = {}
                for manifest_path in manifest:
                    parent_key = str(Path(manifest_path).parent)
                    if parent_key in manifest_dir_identities:
                        continue
                    manifest_dir_identities[parent_key] = self.mount_identity(
                        parent_key,
                    )
                self.source_manifest_dir_identities[source] = manifest_dir_identities
            self.scan_acc["overall_total"] += len(manifest)
            self.runner.push_event(self.job["id"], "progress", {
                "current": 0,
                "total": 0,
                "current_file": (
                    f"Discovered {len(manifest):,} files in source "
                    f"{source_index} of {len(sources)}"
                ),
                "phase": "Discovering sources",
                "phase_current": source_index,
                "phase_total": len(sources),
                "phase_label": "Discovering sources",
            })

    def _discover_manifest(self, source_index, source):
        if self.snapshot_paths_by_root is not None:
            return sorted(
                Path(path)
                for path in self.snapshot_paths_by_root[source]
                if path in self.snapshot_eligible
            )
        errno_mod = self.errno_mod

        def discovery_onerror(exc):
            if exc.errno in (errno_mod.EPERM, errno_mod.EACCES):
                raise exc
            log.warning(
                "Import-in-place discovery error at %s: %s",
                exc.filename, exc,
            )

        return self.discover_source_files(
            source,
            file_types="both",
            recursive=self.plan.recursive,
            onerror=discovery_onerror,
            cancel_check=self._cancel_check,
            progress_callback=lambda checked, found, i=source_index,
            s=source: self._emit_discovery(i, s, checked, found),
        )

    def _publish_overall_total(self):
        job = self.job
        overall_total = self.scan_acc["overall_total"]
        # Publish the overall denominator only after every source has
        # contributed. From this event onward it never changes.
        job["progress"]["current"] = 0
        job["progress"]["total"] = overall_total
        self.runner.update_step(
            job["id"], "scan",
            progress={
                "current": 0,
                "total": overall_total,
            },
            current_file="",
        )
        self.runner.push_event(job["id"], "progress", {
            "current": 0,
            "total": overall_total,
            "current_file": "",
            "phase": "Importing in place",
            "phase_current": None,
            "phase_total": None,
            "phase_label": None,
        })

    # -- per-source scan ---------------------------------------------

    def _scan_sources(self):
        for idx, source in enumerate(self.sources, 1):
            # Discovery can observe a transient pause request and raise
            # ScanCancelled before the runner settles into its paused
            # state. Keep the local outcome authoritative even if the
            # runner is resumed before this loop checks again; later
            # sources do not have frozen manifests in that case.
            if self.cancelled or self._cancel_check():
                self.cancelled = True
                break
            if source in self.source_discovery_failures:
                continue
            self._scan_source(idx, source)
            if self.cancelled:
                break

    def _scan_source(self, idx, source):
        scan_acc = self.scan_acc
        scan_acc["source_index"] = idx
        scan_acc["last_current"] = 0
        scan_acc["last_total"] = len(self.source_manifests[source])
        scope = self._source_scope(source)
        if scope is None:
            return
        self._announce_source(idx, source)
        try:
            self._revalidate_source_mounts(source)
            self._scan_manifest(source, scope)
            self._retain_working_copy_scope(source, scope)
        except Exception as exc:
            if isinstance(exc, self.scan_cancelled) and self._cancel_check():
                self.cancelled = True
                return
            log.exception("In-place import failed for source %s", source)
            self._record_error(f"[{source}] {exc}")
            self._advance_past_failed_source()
        finally:
            self._finish_source(source)

    def _source_scope(self, source):
        """The snapshot restriction for ``source``, or None to skip it."""
        if self.snapshot_paths_by_root is None:
            return _SourceScope()
        restricted_files = {
            path for path in self.snapshot_paths_by_root[source]
            if path in self.snapshot_eligible
        }
        if not restricted_files:
            # The snapshot may consist entirely of files that
            # vanished after discovery. No scanner call will run,
            # but the cached banner count is still stale.
            try:
                invalidate_new_images_after_scan(self.thread_db, source)
            except Exception:
                log.exception(
                    "Failed to invalidate new-image cache for %s",
                    source,
                )
            return None
        restricted_dirs = sorted(
            {os.path.dirname(path) for path in restricted_files}
        )
        restricted_dir_identities = {
            str(Path(directory)): self.mount_identity(directory)
            for directory in restricted_dirs
        }
        return _SourceScope(
            files=restricted_files,
            dirs=restricted_dirs,
            dir_identities=restricted_dir_identities,
        )

    def _announce_source(self, idx, source):
        job = self.job
        sources = self.sources
        phase = (
            f"Importing source {idx} of {len(sources)}: {source}"
            if len(sources) > 1 else "Importing in place"
        )
        self.runner.update_step(
            job["id"], "scan",
            current_file=phase,
            source_index=idx,
        )
        self.runner.push_event(job["id"], "progress", {
            "current": job["progress"].get("current", 0),
            "total": job["progress"].get("total", 0),
            "current_file": phase,
            "phase": phase,
            "phase_current": None,
            "phase_total": None,
            "phase_label": None,
        })

    def _revalidate_source_mounts(self, source):
        errno_mod = self.errno_mod
        # Revalidate the source's mount identity before replaying
        # its frozen manifest. Between discovery and scan a
        # removable/network source (or a local directory) can be
        # detached and replaced at the same path while later
        # sources are still being discovered; ``root_path.is_dir()``
        # inside scanner.scan would still be true, so common camera
        # filenames such as ``DCIM/.../IMG_0001.JPG`` would be
        # cataloged from the wrong volume. The post-scan check that
        # already gates working-copy extraction runs too late to
        # prevent that catalog contamination — do the check here.
        changed_source_mount = self.changed_mount_since_baseline(
            self.source_mount_identities.get(str(Path(source)), {}),
        )
        if changed_source_mount is not None:
            raise FileNotFoundError(
                errno_mod.ENOENT,
                (
                    "source mount changed since discovery "
                    f"({changed_source_mount}); refusing to "
                    "replay frozen manifest against replacement "
                    "filesystem"
                ),
                source,
            )
        # The source-root check above cannot see a nested
        # directory, mount, or symlink that was replaced with an
        # ordinary photo subtree after discovery: the root's own
        # inode stayed the same. Revalidate every parent
        # directory of the frozen manifest here, so the scanner
        # cannot stat the frozen filenames under a substituted
        # subtree and catalog the wrong files.
        changed_manifest_dir = self.changed_mount_since_baseline(
            self.source_manifest_dir_identities.get(source, {}),
        )
        if changed_manifest_dir is not None:
            raise FileNotFoundError(
                errno_mod.ENOENT,
                (
                    "nested directory changed since discovery "
                    f"({changed_manifest_dir}); refusing to "
                    "replay frozen manifest against replacement "
                    "subtree"
                ),
                source,
            )

    def _scan_manifest(self, source, scope):
        plan = self.plan
        # scan() commits rows incrementally and can raise after
        # thousands have landed, so read counts from a sink dict
        # rather than the return value — the same pattern the
        # multi-root scan job uses. Frozen manifests widen the
        # window in which promised files can vanish before their
        # source is processed; the vanished bucket must be
        # surfaced as a source failure or a successful import
        # report would silently follow a partial catalog.
        source_scan_counts = {}
        self.do_scan(
            source, self.thread_db,
            # Photos are cataloged globally. Reuse unchanged
            # records when linking them into another workspace
            # instead of rereading metadata and hashes over NAS.
            incremental=True,
            repair_missing_metadata=True,
            progress_callback=self._progress_cb,
            extract_full_metadata=self.pipeline_cfg.get(
                "extract_full_metadata", True,
            ),
            photo_callback=self._photo_cb,
            photo_merged_callback=self._photo_merged_cb,
            status_callback=self._status_cb,
            recursive=plan.recursive,
            restrict_dirs=scope.dirs,
            restrict_files=scope.files,
            vireo_dir=plan.vireo_dir,
            thumb_cache_dir=plan.thumb_cache_dir,
            cancel_check=self._cancel_check,
            pause_check=self._pause_check,
            cancel_only_check=self._cancel_only_check,
            # Pair companions during each scan, but defer RAW
            # working-copy generation until every source has been
            # cataloged. One combined pass gives the UI a truthful
            # total instead of restarting a 0..N phase per source.
            skip_working_copies=True,
            register_restrict_dirs_as_roots=(
                self.snapshot_paths_by_root is None
            ),
            discovered_files=self.source_manifests[source],
            counts=source_scan_counts,
        )
        vanished_count = source_scan_counts.get("vanished", 0)
        if vanished_count:
            msg = (
                f"[{source}] {vanished_count} file(s) vanished "
                "between discovery and scan; import is incomplete"
            )
            log.warning(
                "In-place import: %d file(s) promised by the "
                "frozen manifest for %s were missing at scan "
                "time",
                vanished_count, source,
            )
            self._record_error(msg)

    def _retain_working_copy_scope(self, source, scope):
        # Retain extraction scope only after the scan returns and
        # only while its paths are still valid. A selected volume
        # can disappear after request validation; handing that
        # stale scope to the deferred extractor would mark every
        # pre-existing RAW row as failed for 24 hours.
        if scope.dirs is not None:
            for directory in scope.dirs:
                if (
                    not self.is_excluded_scan_path(Path(directory))
                    and os.path.isdir(directory)
                ):
                    entry = (directory, "exact")
                    self.working_copy_scope.append(entry)
                    self.working_copy_scope_baselines[entry] = (
                        self.source_mount_baselines.get(
                            str(Path(source)), {},
                        )
                    )
                    entry_identities = dict(
                        self.source_mount_identities.get(
                            str(Path(source)), {},
                        )
                    )
                    directory_key = str(Path(directory))
                    entry_identities[directory_key] = (
                        scope.dir_identities.get(directory_key)
                    )
                    self.working_copy_scope_identities[entry] = entry_identities
        elif (
            not self.is_excluded_scan_path(Path(source))
            and os.path.isdir(source)
        ):
            # scanner.scan converts its root to Path before it
            # catalogs folder strings, which removes lexical
            # trailing separators and ``.`` components. Use that
            # exact spelling for the deferred SQL scope too.
            normalized_source = str(Path(source))
            entry = (
                (normalized_source, "exact")
                if not self.plan.recursive else normalized_source
            )
            self.working_copy_scope.append(entry)
            self.working_copy_scope_baselines[entry] = (
                self.source_mount_baselines.get(normalized_source, {})
            )
            self.working_copy_scope_identities[entry] = (
                self.source_mount_identities.get(normalized_source, {})
            )

    def _advance_past_failed_source(self):
        job = self.job
        scan_acc = self.scan_acc
        # Advance the counter past this source's frozen
        # manifest so the overall denominator is still
        # reached even when the scan failed — a source that
        # disconnected after discovery (scanner raises
        # FileNotFoundError on the missing root) would
        # otherwise leave the progress bar permanently below
        # its promised total. scan_acc["last_total"] holds
        # this source's frozen size; the ``finally`` block
        # below rolls it into ``prior`` via advance_scan_acc.
        if scan_acc["last_current"] < scan_acc["last_total"]:
            scan_acc["last_current"] = scan_acc["last_total"]
            # Emit a progress event now so the bar visibly
            # moves past this source instead of only jumping
            # once a later source's photo_cb re-publishes.
            # The frozen denominator is preserved; ``current``
            # advances by the failed source's manifest size.
            cum_current = (
                scan_acc["prior"] + scan_acc["last_current"]
            )
            cum_total = scan_acc["overall_total"]
            job["progress"]["current"] = cum_current
            job["progress"]["total"] = cum_total
            self.runner.update_step(
                job["id"], "scan",
                progress={"current": cum_current, "total": cum_total},
            )
            self.runner.push_event(job["id"], "progress", {
                "current": cum_current,
                "total": cum_total,
                "current_file": job["progress"].get("current_file", ""),
                "phase": "Importing in place",
                "phase_current": None,
                "phase_total": None,
                "phase_label": None,
            })

    def _finish_source(self, source):
        try:
            invalidate_new_images_after_scan(self.thread_db, source)
        except Exception as cache_exc:
            log.exception(
                "Failed to invalidate new-image cache for %s", source,
            )
            self._record_error(
                f"[{source}] cache invalidation failed after import: "
                f"{cache_exc}"
            )
        # scanner.scan touches disk and may reconcile ghost rows
        # (e.g. a user restored an original before running
        # import-in-place). The pre-scan health-check invalidation
        # only fires when a folder flips missing/ok, so also drop
        # the missing-originals cache once the scan itself has
        # run — even on partial failure, since rows are committed
        # incrementally.
        try:
            self.plan.service.invalidate_missing_originals()
        except Exception:
            log.exception(
                "Failed to invalidate missing-originals cache after in-place import scan of %s",
                source,
            )
        self._advance_scan_acc()

    # -- working copies ----------------------------------------------

    def _extract_deferred_working_copies(self):
        revalidated_scope = self._revalidated_working_copy_scope()
        if revalidated_scope:
            try:
                self.extract_working_copies(
                    self.thread_db,
                    self.plan.vireo_dir,
                    status_callback=self._status_cb,
                    scope=revalidated_scope,
                    cancel_check=self._cancel_check,
                )
            except Exception as exc:
                log.exception(
                    "In-place import working-copy generation failed",
                )
                self._record_error(f"[working copies] {exc}")

    def _revalidated_working_copy_scope(self):
        # A removable source can disappear after its scan succeeded
        # but before the aggregate extract pass runs (later sources
        # were still scanning, or a card was pulled between the loop
        # ending and this call). Revalidate every retained scope
        # entry now so the extractor never reads a vanished volume
        # and stamps 24h ``working_copy_failed_at`` markers on its
        # pre-existing catalog rows.
        revalidated_scope = []
        for entry in self.working_copy_scope:
            if isinstance(entry, tuple):
                path = entry[0]
            else:
                path = entry
            try:
                detached_mount = self.unmounted_since_baseline(
                    self.working_copy_scope_baselines.get(entry, {}),
                )
                changed_mount = self.changed_mount_since_baseline(
                    self.working_copy_scope_identities.get(entry, {}),
                )
                still_available = (
                    not self.is_excluded_scan_path(Path(path))
                    and detached_mount is None
                    and changed_mount is None
                    and os.path.isdir(path)
                )
            except OSError:
                still_available = False
                detached_mount = None
                changed_mount = None
            if still_available:
                revalidated_scope.append(entry)
            else:
                log.info(
                    "Skipping deferred working-copy scope %s: no "
                    "longer present, excluded, or mount changed%s",
                    path,
                    (
                        f" ({detached_mount or changed_mount})"
                        if detached_mount or changed_mount
                        else ""
                    ),
                )
        return revalidated_scope

    # -- result ------------------------------------------------------

    def _finish(self):
        if self.snapshot_paths is not None:
            # Pairing can merge a JPEG photo into its RAW and delete the
            # JPEG's row after photo_cb saw it (``_photo_merged_cb`` follows
            # that). Resolve the frozen paths again anyway so collections,
            # tags, and a chained Process job receive durable catalog IDs.
            catalog_ids_after = self._active_photo_ids_by_path()
            self.photo_ids = list(dict.fromkeys(
                catalog_ids_after[path]
                for path in self.snapshot_paths
                if path in self.indexed_paths and path in catalog_ids_after
            ))
        indexed = len(self.photo_ids)
        snapshot_unindexed = sorted(
            self.snapshot_eligible - self.indexed_paths
        ) if self.snapshot_paths is not None else []
        if self.cancelled or self._cancel_check():
            return self._cancelled_result(indexed, snapshot_unindexed)
        return self._completed_result(indexed, snapshot_unindexed)

    def _snapshot_result_fields(self, snapshot_unindexed):
        known_before = set(self.snapshot_known_before)
        return {
            "source_snapshot_id": self.plan.source_snapshot_id,
            "requested": self.snapshot_requested,
            "imported": len(self.indexed_paths - known_before),
            "already_cataloged": len(self.indexed_paths & known_before),
            "missing": len(self.snapshot_missing),
            "missing_paths": self.snapshot_missing[:100],
            "unreadable": len(self.snapshot_unreadable),
            "unreadable_paths": self.snapshot_unreadable[:100],
            "unindexed": len(snapshot_unindexed),
            "unindexed_paths": snapshot_unindexed[:100],
        }

    def _apply_import_tags(self, result):
        plan = self.plan
        plan.service._apply_import_tags(
            plan.active_ws, self.photo_ids, plan.import_tags,
            plan.location_from_gps, result, job=self.job, runner=self.runner,
        )

    def _cancelled_result(self, indexed, snapshot_unindexed):
        self.runner.update_step(
            self.job["id"], "scan", status="cancelled",
            summary=f"{indexed} photos (cancelled)",
        )
        result = {
            "mode": "in_place",
            "ok": False,
            "cancelled": True,
            "discovered": indexed,
            "indexed": indexed,
            "failed": len(self.root_errors),
            "errors": self.root_errors,
            "photo_ids": self.photo_ids,
        }
        if self.snapshot_paths is not None:
            result.update(self._snapshot_result_fields(snapshot_unindexed))
        self._apply_import_tags(result)
        return result

    def _completed_result(self, indexed, snapshot_unindexed):
        snapshot_missing = self.snapshot_missing
        snapshot_unreadable = self.snapshot_unreadable
        metadata_warning = scan_metadata_warning()
        summary = f"{indexed} photos"
        if metadata_warning:
            summary += f" — {metadata_warning}"
        snapshot_failures = (
            len(snapshot_missing)
            + len(snapshot_unreadable)
            + len(snapshot_unindexed)
        )
        all_errors = list(self.root_errors)
        if snapshot_missing:
            all_errors.append(
                f"{len(snapshot_missing)} snapshot file"
                f"{'s were' if len(snapshot_missing) != 1 else ' was'} "
                "missing at import time"
            )
        if snapshot_unreadable:
            all_errors.append(
                f"{len(snapshot_unreadable)} snapshot file"
                f"{'s were' if len(snapshot_unreadable) != 1 else ' was'} "
                "unreadable at import time"
            )
        if snapshot_unindexed:
            all_errors.append(
                f"{len(snapshot_unindexed)} available snapshot file"
                f"{'s were' if len(snapshot_unindexed) != 1 else ' was'} "
                "not indexed"
            )
        self.runner.update_step(
            self.job["id"], "scan",
            status="failed" if all_errors else "completed",
            summary=summary,
            error=all_errors[0] if all_errors else None,
            error_count=len(all_errors) if all_errors else None,
        )
        result = {
            "mode": "in_place",
            "ok": not all_errors,
            "discovered": indexed,
            "indexed": indexed,
            "failed": (
                snapshot_failures
                if self.snapshot_paths is not None else len(self.root_errors)
            ),
            "errors": all_errors,
            "photo_ids": self.photo_ids,
        }
        if self.snapshot_paths is not None:
            result.update(self._snapshot_result_fields(snapshot_unindexed))
        self._apply_import_tags(result)
        # Pause before publishing the collection or handing off to a
        # child job. Once the handoff starts, late parent requests must
        # not claim they can stop the independently running child.
        if not self.runner.begin_uncancellable(self.job["id"]):
            result["cancelled"] = True
        self.plan._chain_after_import(self.job, result)
        return result
