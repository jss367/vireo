"""Folder moves: the move-folder job, its enqueue guard, and the chained enqueue.

``FolderMoves`` is built once per app in ``create_app``. ``start_job`` runs
the move-folder job for both the manual ``/api/jobs/move-folder`` launcher
and the chained import→process→move hook; ``enqueue_job`` is that hook's
entry point (snapshot remote target, no request context); ``guard_error``
refuses a move that would race a local-workspace or local-folder copy; and
``pending_local_workspace_transition`` finds a queued or running
local-copy transition job, which the scan launchers also check.
Nothing here touches Flask's request or app context except the
``get_db`` fallback when ``pending_local_workspace_transition`` is called
without an explicit db.
"""

from __future__ import annotations

import logging
import os
import posixpath
import time

from db import Database
from services.local_folder import (
    LOCAL_FOLDER_JOB_TYPES,
    local_root_for_folder,
    local_root_under_folder,
    workspace_ids_for_folder_tree,
)
from services.local_workspace import (
    LOCAL_WORKSPACE_JOB_TYPES,
    folder_has_local_workspace,
    stage_boundary_lock,
)

log = logging.getLogger(__name__)


# How many planned capture-date folders a date-organized move job snapshots
# into its config for the jobs panel. The panel lists these and reports the
# real total separately, so the route stays readable (and the job row small)
# even when a source folder spans hundreds of dates.
MOVE_DATE_DEST_PREVIEW_LIMIT = 8


class FolderMoves:
    """Move-folder jobs and the guards that gate them.

    ``get_runner`` is called lazily (tests swap ``app._job_runner``);
    ``get_db`` returns the request-scoped database and is only used when
    ``pending_local_workspace_transition`` gets no explicit db; ``config``
    is the Flask app's config mapping, read when a job runs;
    ``invalidate_missing_originals`` drops the app's Missing Originals
    cache (owned by ``create_app``).
    """

    def __init__(
        self, *, get_runner, get_db, db_path, config,
        invalidate_missing_originals,
    ):
        self._get_runner = get_runner
        self._get_db = get_db
        self._db_path = db_path
        self._config = config
        self._invalidate_missing_originals = invalidate_missing_originals

    def pending_local_workspace_transition(self, workspace_id, db=None):
        """Return the queued/running local-workspace transition job, or None.

        ``db``: pass an explicit Database when calling off the request
        thread (job threads have no request context); defaults to the
        request-scoped db via ``get_db()``.
        """
        # ``has_local_workspace`` only observes the ``local_workspaces`` row
        # a stage worker inserts once it actually runs; a stage/sync/discard
        # that has been enqueued but not yet reached that insert would leave
        # the row absent. A scan or move-folder enqueued in that window
        # passes its own guard and then rebases the catalog after the
        # transition worker later claims the workspace, so the folder /
        # workspace_folders / folders rows those jobs write end up outside
        # the manifest and local_workspace_folders. Detecting the pending
        # transition job in the runner queue closes that race at enqueue.
        if workspace_id is None:
            return None
        for job in self._get_runner().list_jobs():
            if job.get("status") not in (
                "queued", "running", "pausing", "paused",
            ):
                continue
            job_type = job.get("type")
            if job_type in LOCAL_WORKSPACE_JOB_TYPES and job.get("workspace_id") == workspace_id:
                return job
            if job_type in LOCAL_FOLDER_JOB_TYPES:
                if job.get("workspace_id") == workspace_id:
                    return job
                config = job.get("config") or {}
                root_ids = (config.get("root_folder_ids") or []) if isinstance(config, dict) else []
                if db is None:
                    db = self._get_db()
                if any(
                    workspace_id in workspace_ids_for_folder_tree(db, int(root_id))
                    for root_id in root_ids
                ):
                    return job
        return None

    def guard_error(self, guard_db, folder_id):
        """Return the error message blocking a folder move, or None.

        Context-free on purpose: takes the db explicitly and touches no
        Flask request/app context, so the chained completion hook can run
        it from a job thread with a thread db.
        """
        # A folder covered by any workspace's local_workspace_folders row has
        # its folders.path rebased into that workspace's managed copy; a
        # concurrent workspace-membership guard would refuse to touch the
        # folders row for exactly this reason. Moving it here (via
        # db.move_folder_path) would move or delete the managed copy and
        # rewrite the catalog while local_workspace_folders and the manifest
        # still expect the pre-move layout, so the owning workspace's next
        # status falls into missing-local recovery and sync/discard can no
        # longer restore. Reject the job before enqueue.
        with stage_boundary_lock():
            local_root_id = local_root_for_folder(guard_db, folder_id)
            if local_root_id is not None:
                return (
                    "Cannot move this folder while it has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            # A descendant local copy has already had its folders.path rebased
            # under local-folders/, so a folders.path subtree walk from this
            # ancestor no longer sees it — but local_folder_mappings.source_path
            # still records the original location. Without this check the move
            # job would move/delete the original source directory out from
            # under the manifest, leaving sync/discard unable to restore.
            descendant_root_id = local_root_under_folder(guard_db, folder_id)
            if descendant_root_id is not None:
                return (
                    "Cannot move this folder while a subfolder has a shared local copy. "
                    "Sync or discard the local copy from any linked workspace first."
                )
            staged_here, staged_owner_ws = folder_has_local_workspace(
                guard_db, folder_id,
            )
            if staged_here:
                return (
                    f"Cannot move this folder — workspace {staged_owner_ws} has it "
                    "staged locally. Switch to that workspace and sync or discard the "
                    "local copy first."
                )
            # ``folder_has_local_workspace`` only sees a completed stage
            # claim. A stage/sync/discard queued or running against a
            # workspace that contains this folder hasn't yet written
            # local_workspace_folders, so the read-only guard above passes
            # even though the transition worker is about to rebase this
            # folder's paths. Enqueueing the move now would rewrite the
            # ``folders`` row out from under the pending transition —
            # missing-local recovery on the next status. Refuse until the
            # transition finishes.
            row = guard_db.conn.execute(
                "SELECT workspace_id FROM workspace_folders WHERE folder_id = ?",
                (folder_id,),
            ).fetchall()
            for ws_row in row:
                pending = self.pending_local_workspace_transition(
                    int(ws_row["workspace_id"]), db=guard_db)
                if pending:
                    return (
                        f"Wait for the {pending['type']} job on workspace "
                        f"{int(ws_row['workspace_id'])} to finish before moving this "
                        "folder; otherwise the move would run on paths that workspace "
                        "is about to claim."
                    )
        return None

    def start_job(self, runner, workspace_id, *, folder_id,
                  destination, display_dest, destination_name,
                  source_path, resolved_destination,
                  merge, remote, developed_dir, folder_template="",
                  date_destinations=None,
                  chained_from=None, serialize_lock=None,
                  allow_tracked_merge=False,
                  managed_staging_root=None, mount_baseline=None,
                  mount_identities=None):
        """Enqueue a move-folder job and return its job id.

        Shared by the move-folder endpoint and the chained
        process-completion hook (which runs on a job thread, so this
        must not touch Flask request/app context).

        ``serialize_lock``: optional ``threading.Lock`` shared by a batch
        of chained moves; when given, the transfer itself runs under the
        lock so batch-mates execute one at a time (see the why-comment at
        the acquire site). The job still enqueues — and its id exists —
        immediately.

        ``allow_tracked_merge``: passed through to ``move_folder``. The
        chained import→process→move hook opts in (see the why-comment in
        ``enqueue_job``); the manual move endpoint keeps the
        default refusal of tracked destinations.

        ``date_destinations``: the planned capture-date folders for a
        date-organized move (``path``/``relative_path``/``photo_count`` per
        entry, as produced by ``plan_folder_date_moves``). Snapshotted into
        the job config so the jobs panel can name the folders photos actually
        land in rather than only the selected root.
        """
        def work(job):
            from move import move_folder, move_folder_by_date

            thread_db = Database(self._db_path)
            thread_db.set_active_workspace(workspace_id)

            job["_start_time"] = time.time()

            last_phase = {"value": None}

            def progress_cb(current, total, filename, phase="Moving folder"):
                # Only update keys JobRunner pre-seeds in job["progress"]
                # (current/total/current_file). Do NOT insert "phase" here:
                # this runs on the worker thread outside the runner lock, and
                # adding a new key races _snapshot_job's locked dict() copy
                # ("dictionary changed size during iteration"). push_event
                # below mirrors phase onto job["progress"] under the lock.
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                # The copy phase fires once per file; on a large folder that
                # would flood the SSE stream and tie up Flask threads. Throttle
                # to every 10th file, but always emit on a phase change and on
                # the first/last file so the panel never looks stalled.
                phase_changed = phase != last_phase["value"]
                last_phase["value"] = phase
                if not phase_changed and current % 10 != 0 \
                        and current not in (1, total):
                    return
                runner.push_event(job["id"], "progress", {
                    "current": current,
                    "total": total,
                    "current_file": filename,
                    "phase": phase,
                })

            # The chained moves from one import all rsync to the same NAS,
            # and runner.start gives each its own thread immediately — so
            # N folders would mean N concurrent rsyncs, each honoring
            # --bwlimit individually and together consuming N× the
            # configured bandwidth budget. The chain hands every job in
            # the batch one shared lock so the transfers run one at a
            # time while all N jobs (and their ids) still enqueue up
            # front.
            if serialize_lock is not None and not serialize_lock.acquire(blocking=False):
                # UI transparency: a job blocked on the batch lock
                # must say why it isn't moving anything yet.
                runner.push_event(job["id"], "progress", {
                    "current": 0,
                    "total": 0,
                    "current_file": "",
                    "phase": (
                        "Waiting for an earlier chained move to finish"
                    ),
                })
                # Poll instead of blocking outright: this wait is the
                # one boundary where Cancel can be honored without
                # touching mid-transfer semantics — a blocking acquire
                # would run the whole transfer anyway after the user
                # pressed Stop.
                while not serialize_lock.acquire(timeout=0.5):
                    if runner.is_cancelled(job["id"]):
                        # Returning with the lock NOT held, before the
                        # try below — so the release in its finally
                        # never fires on this path.
                        return {
                            "ok": False, "moved": 0, "errors": [],
                            "summary": (
                                "Cancelled before transfer started"
                            ),
                        }
            # Cancel check with the lock held, covering two paths:
            # (1) the waiter's ``acquire(timeout=0.5)`` returned True in
            # the same 0.5s window a cancel landed in, exiting the loop
            # without the in-loop check running; (2) a chained move whose
            # thread started AFTER the earlier holder already released
            # the batch lock — its non-blocking acquire succeeds so it
            # never enters the wait loop, but its ``/cancel`` may have
            # already been accepted while the thread was still queued.
            # Either way, don't start the transfer.
            if serialize_lock is not None and runner.is_cancelled(job["id"]):
                serialize_lock.release()
                return {
                    "ok": False, "moved": 0, "errors": [],
                    "summary": (
                        "Cancelled before transfer started"
                    ),
                }
            try:
                check_mount = None
                if managed_staging_root and not runner.begin_uncancellable(job["id"]):
                    return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
                if mount_baseline is not None:
                    from import_staging import check_staged_mount

                    def check_mount():
                        check_staged_mount(resolved_destination, mount_baseline, mount_identities)
                    check_mount()
                if folder_template:
                    result = move_folder_by_date(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        folder_template=folder_template,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                    )
                else:
                    result = move_folder(
                        db=thread_db,
                        folder_id=folder_id,
                        destination=destination,
                        progress_cb=progress_cb,
                        developed_dir=developed_dir,
                        merge=merge,
                        remote=remote,
                        destination_name=destination_name,
                        allow_tracked_merge=allow_tracked_merge,
                        thumb_cache_dir=self._config["THUMB_CACHE_DIR"],
                        **({"verify_contents": True} if managed_staging_root and not remote else {}),
                        **({"pre_commit_check": check_mount} if check_mount else {}),
                    )
            finally:
                if serialize_lock is not None:
                    serialize_lock.release()

            # Tell the JobRunner whether the move actually succeeded. Without
            # this the runner marks any normal return "completed" — so a move
            # that copied nothing because rsync timed out used to read as
            # "completed, 0 errors" in the history. A `needs_merge` return is
            # NOT a failure: it's a soft signal that the destination already
            # exists and the UI should re-prompt for a merge/resume, so leave
            # it for the caller without flagging the job failed.
            if not result.get("needs_merge"):
                errors = result.get("errors") or []
                moved = result.get("moved", 0)
                if errors and moved == 0:
                    result["ok"] = False
                    result["summary"] = f"Move failed — {errors[0]}"
                else:
                    cleanup_error = result.get("cleanup_error")
                    result["ok"] = True
                    result["summary"] = (
                        f"Moved {moved} photo{'s' if moved != 1 else ''}"
                        + (f", {len(errors)} error(s)" if errors else "")
                        + (
                            f"; cleanup failed: {cleanup_error}"
                            if cleanup_error else ""
                        )
                    )
            if result.get("ok"):
                if managed_staging_root:
                    from path_guard import contains_resolved
                    # Remove only empty staging ancestors. Failed transfers and
                    # concurrent sibling moves keep their originals intact.
                    parent = os.path.dirname(source_path)
                    root = os.path.realpath(managed_staging_root)
                    while contains_resolved(root, parent):
                        try:
                            os.rmdir(parent)
                        except OSError:
                            break
                        if os.path.realpath(parent) == root:
                            break
                        parent = os.path.dirname(parent)
                try:
                    self._invalidate_missing_originals()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after move-folder job",
                    )
            return result

        job_config = {
            "folder_id": folder_id, "destination": display_dest, "merge": merge,
            # Snapshot both ends of the move when it is enqueued. The source
            # catalog row is rewritten after a successful move, so resolving
            # it later would make completed jobs misleading. Likewise,
            # ``destination`` above is only the selected parent; the jobs UI
            # needs the actual landing path (including a rename/source leaf).
            "source_path": source_path,
            "resolved_destination": resolved_destination,
        }
        if folder_template:
            job_config["folder_template"] = folder_template
        if date_destinations:
            # Cap the stored list: a multi-year source folder can plan
            # thousands of date folders, and the whole config is serialized
            # into the job row and every status poll. The panel shows the
            # first few and reports the true totals from the counts below,
            # which are computed over the full plan.
            job_config["date_destinations"] = [
                {
                    "path": item["path"],
                    "relative_path": item["relative_path"],
                    "photo_count": item["photo_count"],
                }
                for item in date_destinations[:MOVE_DATE_DEST_PREVIEW_LIMIT]
            ]
            job_config["date_destination_count"] = len(date_destinations)
            job_config["date_photo_count"] = sum(
                item["photo_count"] for item in date_destinations)
        if destination_name:
            job_config["destination_name"] = destination_name
        if remote:
            # Surface that this is an SSH transfer (and to where) so the job
            # panel can show it, per the UI-transparency rule.
            job_config["remote"] = {
                "host": remote["host"], "user": remote["user"],
                "ssh_dest_base": remote["ssh_dest_base"],
                "mount_dest_base": remote["mount_dest_base"],
                "bwlimit_kbps": remote["bwlimit_kbps"],
            }
        if chained_from:
            # Provenance for the jobs panel: this move was started by a
            # chained process run's completion hook, not by hand.
            job_config["chained_from"] = chained_from

        def staged_work(job):
            runner.push_event(job["id"], "progress", {
                "current": 0, "total": 0, "current_file": "",
                "phase": "Waiting for workspace jobs to finish before sending to NAS",
            })
            if not runner.wait_for_workspace_transfer(job["id"]):
                return {"ok": False, "moved": 0, "errors": [], "summary": "Cancelled before transfer started"}
            return work(job)

        return runner.start(
            "move-folder", staged_work if managed_staging_root else work,
            config=job_config,
            workspace_id=workspace_id,
            **({"workspace_transfer_batch": managed_staging_root} if managed_staging_root else {}),
        )

    def enqueue_job(self, thread_db, runner, workspace_id, *,
                    folder_id, subpath, target,
                    chained_from=None, serialize_lock=None):
        """Enqueue a chained remote move for one imported folder.

        Job-thread path into move-folder (no request context). ``target`` is
        the snapshot captured when the import was enqueued — deliberately NOT
        re-resolved from Settings here, so a mid-chain edit cannot redirect
        the move. Raises on any precondition failure — the caller records the
        failure per folder rather than aborting the batch.
        """
        import config as cfg
        import move as move_mod

        mount_path = (target.get("mount_path") or "").strip()
        if not mount_path:
            raise RuntimeError(
                "remote target has no local mount path, so moved photos "
                "couldn't stay in your library — add one under Settings → "
                "Remote targets")
        if not os.path.isabs(mount_path):
            raise RuntimeError(
                f"remote target's local mount path isn't absolute "
                f"(\"{mount_path}\") — fix it under Settings → Remote targets")
        guard = self.guard_error(thread_db, folder_id)
        if guard:
            raise RuntimeError(guard)
        effective_cfg = thread_db.get_effective_config(cfg.load())
        if target.get("transport") == "mounted":
            folder = thread_db.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (folder_id,),
            ).fetchone()
            if not folder:
                raise RuntimeError("folder no longer exists")
            destination = os.path.join(mount_path, *posixpath.dirname(subpath).split("/"))
            return self.start_job(
                runner, workspace_id, folder_id=folder_id,
                destination=destination, display_dest=destination,
                destination_name="", source_path=folder["path"],
                resolved_destination=os.path.join(destination, os.path.basename(folder["path"])),
                merge=True, remote=None,
                developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
                chained_from=chained_from, serialize_lock=serialize_lock,
                allow_tracked_merge=True,
                managed_staging_root=target.get("managed_staging_root"),
                mount_baseline=target.get("mount_baseline"),
                mount_identities=target.get("mount_identities"),
            )
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        if not rsync_bin:
            raise RuntimeError("no usable GNU rsync for remote moves")
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        if not ssh_bin:
            raise RuntimeError("OpenSSH client not found")
        # ``move_folder`` lands the source folder INSIDE the destination,
        # keeping the folder's own name — so to mirror the archive layout
        # (``<local_archive_root>/2026/trip`` → ``<remote_path>/2026/trip``)
        # the spec's subpath must be the folder's PARENT ("2026"), not the
        # full archive-relative subpath, or the leaf would double up
        # ("2026/trip/trip").
        remote = move_mod.build_remote_move_spec(
            target, posixpath.dirname(subpath), rsync_bin, ssh_bin)
        folder = thread_db.conn.execute(
            "SELECT path, name FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not folder:
            raise RuntimeError("folder no longer exists")
        landing_name = folder["name"] \
            or os.path.basename(folder["path"].rstrip("/\\"))
        resolved_destination = move_mod.rsync_dest_spec(
            target,
            posixpath.join(remote["ssh_dest_base"], landing_name),
        )
        return self.start_job(
            runner, workspace_id,
            folder_id=folder_id,
            destination=remote["mount_dest_base"],
            display_dest=move_mod.rsync_dest_spec(
                target, remote["ssh_dest_base"]),
            destination_name="",
            source_path=folder["path"],
            resolved_destination=resolved_destination,
            merge=True,
            remote=remote,
            developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
            chained_from=chained_from,
            serialize_lock=serialize_lock,
            # Re-importing more photos into an existing shoot folder is the
            # NORMAL flow, and its chained move lands exactly on the tracked
            # NAS copy created by the previous chain run. Without tracked-
            # merge the move would refuse ("Destination overlaps a folder
            # Vireo already manages") and strand the new photos locally.
            # Opting in uses move_folder's exact-overlap reconciliation
            # (fold the new rows into the existing archive rows); the
            # pre-copy content-conflict scan still refuses any same-name
            # file whose bytes differ, and manual moves keep the default
            # refusal.
            allow_tracked_merge=True,
            managed_staging_root=target.get("managed_staging_root"),
        )
