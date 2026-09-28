"""Admission and background workflow for archive-copy imports."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import path_guard
from db import Database
from services.imports import ImportFailure
from services.local_folder import local_copy_scan_conflict, stage_pending_source_paths
from services.local_workspace import stage_boundary_lock
from services.pipeline_launch import resolve_remote_archive_target

if TYPE_CHECKING:
    from services.imports import ImportService

log = logging.getLogger(__name__)


def enqueue_import_photos(service: ImportService, db: Database, body: dict) -> dict | ImportFailure:
    """Copy card photos to the archive, hash-verify, and catalog incrementally.

    Distinct from ``POST /api/jobs/import`` (Lightroom catalog import),
    which keeps its route and shape. No pipeline slot involvement —
    imports are I/O-bound and must not queue behind a GPU run; that
    coupling is exactly what the split removes.
    """
    from image_loader import is_excluded_scan_path
    from ingest import _is_unsafe_path

    dependency_error = service._validate_import_metadata_dependency(body)
    if dependency_error is not None:
        return dependency_error

    sources = body.get("sources")
    if isinstance(sources, str):
        sources = [sources]
    if not sources or not isinstance(sources, list) or not all(
        isinstance(s, str) and s for s in sources
    ):
        return ImportFailure("sources must be a non-empty list of paths")
    for s in sources:
        # Pre-stat rejection of other-app bundles: os.path.isdir on a
        # .photoslibrary path itself trips the macOS TCC prompt.
        if is_excluded_scan_path(s):
            return ImportFailure(
                f"source is inside a macOS app-managed library and "
                f"cannot be imported: {s}"
            )
        if not os.path.isdir(s):
            return ImportFailure(f"source directory not found: {s}")

    include_paths = body.get("include_paths")
    previewed_count = body.get("previewed_count")
    checked_count = body.get("checked_count")

    # All three travel together or none do. A partial set would fabricate
    # drift figures and then blow up on None inside the job.
    provided = [v is not None
                for v in (include_paths, previewed_count, checked_count)]
    if any(provided) and not all(provided):
        return ImportFailure(
            "include_paths, previewed_count and checked_count must be "
            "sent together", 400,
        )

    if include_paths is not None:
        if not isinstance(include_paths, list) or not include_paths:
            return ImportFailure("include_paths must be a non-empty list", 400)
        if any(not isinstance(p, str) or not p for p in include_paths):
            return ImportFailure(
                "include_paths must contain non-empty strings", 400,
            )
        # bool is a subclass of int — {"previewed_count": true} would
        # otherwise sail through as 1.
        for name, val in (("previewed_count", previewed_count),
                          ("checked_count", checked_count)):
            if type(val) is not int or val < 0:
                return ImportFailure(
                    f"{name} must be a non-negative integer", 400,
                )
        include_paths = set(include_paths)
        if len(include_paths) > previewed_count:
            return ImportFailure(
                "include_paths cannot exceed previewed_count", 400,
            )
        if checked_count > len(include_paths):
            return ImportFailure(
                "checked_count cannot exceed include_paths", 400,
            )

        # Containment is LEXICAL by design. normpath catches the real
        # threat (a client naming files outside the chosen folders:
        # "/src/../etc/passwd" collapses and fails). It deliberately does
        # not resolve symlinks — discover_source_files returns symlinked
        # files inside a source and ingest() copies them today, so
        # realpath here would newly reject working imports. Do not
        # "harden" this without reading the spec's §4.
        norm_sources = [os.path.normpath(s) for s in sources]
        for p in include_paths:
            np = os.path.normpath(p)
            try:
                ok = any(os.path.commonpath([np, s]) == s
                         for s in norm_sources)
            except ValueError:
                # Mixed absolute/relative — a containment failure, not a 500.
                ok = False
            if not ok:
                return ImportFailure(
                    f"include_paths contains a path outside the selected "
                    f"source folders: {p}", 400,
                )

    # Remote (SSH) archive destination — mirrors the pipeline route's
    # remote-target request shape (remote_target_id + subpath). The card
    # is rsynced to remote_path/subpath and cataloged at
    # mount_path/subpath; ``destination`` is set to the resolved local
    # mount path so every downstream guard (destination-inside-source,
    # scan, catalog) applies to the mount exactly as for a local import.
    remote_target_id = (body.get("remote_target_id") or "").strip()
    remote_subpath = body.get("remote_subpath", "")
    if remote_subpath and not isinstance(remote_subpath, str):
        return ImportFailure("remote_subpath must be a string")
    destination = body.get("destination")
    remote_archive_config = None
    if remote_target_id and destination:
        return ImportFailure(
            "destination and remote_target_id are mutually exclusive — "
            "pick a local archive path or a saved remote target, not both"
        )
    if remote_subpath and not remote_target_id:
        return ImportFailure("remote_subpath requires remote_target_id")
    after_process_move = body.get("after_process_move")
    local_processing = body.get("local_processing", False)
    if not isinstance(local_processing, bool):
        return ImportFailure("local_processing must be a boolean")
    defer_nas_transfer = body.get("defer_nas_transfer", False)
    if not isinstance(defer_nas_transfer, bool):
        return ImportFailure("defer_nas_transfer must be a boolean")
    if defer_nas_transfer and not local_processing:
        return ImportFailure("Keeping photos local until review requires local processing")
    if local_processing and after_process_move is not None:
        return ImportFailure("Choose automatic local processing or a local archive move, not both")
    if after_process_move is not None and remote_target_id:
        return ImportFailure(
            "after_process_move requires a local archive destination — "
            "a remote-destination import already lands on the NAS"
        )
    if remote_target_id:
        # Refuse at request time when no GNU rsync exists or the
        # target is unknown/unsafe — starting a job guaranteed to
        # fail its transfer helps nobody (mirrors the pipeline and
        # move-folder endpoints).
        remote_archive_config, rsync_bin, err = (
            resolve_remote_archive_target(
                db, remote_target_id, remote_subpath,
                json_error=ImportFailure,
            )
        )
        if err is not None:
            return err
        # Catalog at the resolved local mount path.
        destination = remote_archive_config["mount_final"]

    if not destination:
        return ImportFailure("destination required")
    if not os.path.isabs(destination):
        return ImportFailure("destination must be an absolute path")

    # Reject destinations that are equal to, or nested under, any source
    # (after realpath so a symlink can't slip past). The importer copies
    # every card file into the destination and marks the card safe to
    # format once ``copied + skipped_duplicate == discovered``; if the
    # destination lives inside the card, formatting the card also erases
    # the supposed archive copy, so allowing this is a data-loss trap.
    # Case-fold handling (darwin/win32 unconditional, Linux per-mount
    # probe) lives in path_guard — see its module docstring and the
    # fs_is_case_insensitive() docstring for the full rationale
    # (PR #1107 review).
    try:
        dest_real = os.path.realpath(destination)
    except OSError as e:
        return ImportFailure(f"destination cannot be resolved: {e}")
    for s in sources:
        try:
            source_real = os.path.realpath(s)
        except OSError:
            # Source unresolvable — the os.path.isdir check above
            # already handled non-existent sources; nothing more to say.
            continue
        if path_guard.contains_resolved(source_real, dest_real):
            return ImportFailure(
                f"destination cannot be inside a source directory "
                f"(destination={destination!r}, source={s!r}); "
                f"formatting the card would erase the archive copy"
            )
    # The copied files are catalogued under the destination; if a local
    # copy covers it, they would land beside rows the catalog only knows
    # by their local path.
    #
    # ``include_descendants`` defaults to True. A staged folder beneath
    # the destination is not automatically safe just because the import
    # only walks the dated folders it writes: ``folder_template`` can
    # render a dated folder that coincides with the staged source
    # (``%Y-%m-%d`` when ``/archive/2024-05-01`` is staged and a card
    # holds a photo from that day), and then the import copies into the
    # original source and scans it. Refuse and let the user sync or
    # discard the local copy first.
    db_for_conflict = db
    card_runner = service.get_runner()
    with stage_boundary_lock():
        pending_sources = stage_pending_source_paths(
            card_runner.list_jobs if card_runner is not None else None,
            db_for_conflict,
        )
        conflict = local_copy_scan_conflict(
            db_for_conflict, [destination],
            active_workspace_id=db_for_conflict._active_workspace_id,
            pending_stage_sources=pending_sources,
        )
    if conflict:
        return ImportFailure(conflict, 409)
    # Re-checked atomically with ``runner.start`` below so a stage
    # request that races between the pre-flight release and job
    # registration still blocks on the same boundary lock.
    _import_photos_conflict_paths = [destination]

    folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
    if folder_template and _is_unsafe_path(folder_template):
        return ImportFailure(
            "folder_template must be a relative path without '..' or "
            "backslashes"
        )

    # After-import strategy: validated at enqueue (failing the chain
    # hours later is the old pipeline's mistake). Key present -> null
    # means import-only, a string must name a real strategy. Key
    # omitted -> default from the workspace's pipeline.default_process_id
    # (nullable, same vocabulary). Stored in the job config for the
    # PR 3 chaining hook; the import job itself never reads it.
    # Resolve and validate BEFORE creating any new workspace so a bad
    # value doesn't leave an orphan Archive Import behind. When the
    # request omits the key and asks to create a new workspace, we can't
    # read the workspace-scoped default (the workspace doesn't exist yet,
    # and reading get_effective_config off the previously-active
    # workspace would leak its override into the new-workspace import).
    # A brand-new workspace has no config_overrides, so the effective
    # default is just the global config value — read that directly.
    import config as cfg

    explicit_after_import = "after_import" in body
    if explicit_after_import:
        after_import = body.get("after_import")
    elif "new_workspace_name" in body:
        after_import = (
            cfg.load().get("pipeline", {}).get("default_process_id")
        )
    else:
        effective_cfg = db.get_effective_config(cfg.load())
        after_import = (
            effective_cfg.get("pipeline", {}).get("default_process_id")
        )
    # Recovery-retry imports carry forward the photo IDs a previous
    # attempt already landed, so the after-import chain covers the
    # complete original scope even though those files are skipped as
    # duplicates in this run. Validated at request time so a
    # malformed retry body is rejected before a job is created.
    #
    # ``parent_import_job_id`` binds this retry to the failed run
    # whose scope it inherits: the server verifies the parent is an
    # import job in the same workspace, then constrains
    # ``carry_photo_ids`` to IDs the parent actually imported (or
    # itself inherited from an earlier retry). Without this binding
    # an API caller could inject arbitrary positive integers into
    # the after-import chain scope — those IDs would then flow into
    # ``_record_import_collection``'s scope and, with an
    # ``after_process_move`` chain, into the folder lookup that
    # decides which folders the NAS transfer sweeps up, potentially
    # moving folders outside the active workspace. Refuse the
    # request rather than silently permit it.
    #
    # Resolved BEFORE ``_validate_after_import`` so a retry whose saved
    # process was deleted between the failed run and this retry can
    # still start: the parent's frozen ``after_import_snapshot`` is a
    # legitimate substitute for the deleted process's stage flags, and
    # rejecting the retry with "unknown process id" here would strand
    # every deleted-process retry outright.
    parent_id_raw = body.get("parent_import_job_id")
    parent_config = None
    parent_resume = None
    parent_allowed_ids = None
    parent_allowed_fingerprints = None
    parent_source_snapshots = None
    if parent_id_raw is not None:
        if not isinstance(parent_id_raw, str) or not parent_id_raw.strip():
            return ImportFailure(
                "parent_import_job_id must be a non-empty string"
            )
        if "new_workspace_name" in body:
            return ImportFailure(
                "A recovery retry cannot create a new workspace; "
                "retry in the original import's workspace or start "
                "a new import instead."
            )
        (
            parent_config,
            parent_allowed_ids,
            parent_allowed_fingerprints,
            parent_source_snapshots,
            parent_resume,
            parent_err,
        ) = service._validate_parent_import_job(
            parent_id_raw.strip(), db._active_workspace_id, db,
        )
        if parent_err is not None:
            return parent_err

    # Skip the "process still exists" check ONLY when the parent has a
    # frozen snapshot for THIS exact process id — that snapshot will
    # substitute for db.resolve_process() below. A retry that changed
    # the process id must still go through normal existence validation.
    parent_has_snapshot_for_after_import = (
        parent_config is not None
        and parent_config.get("after_import") == after_import
        and parent_config.get("after_import_snapshot") is not None
    )
    err = service._validate_after_import(
        after_import, db,
        allow_missing=parent_has_snapshot_for_after_import,
    )
    if err is not None:
        return err

    file_types = body.get("file_types", "both")
    skip_duplicates = bool(body.get("skip_duplicates", True))
    verify_by_hash = bool(body.get("verify_by_hash", False))
    trust_likely_duplicates = bool(
        body.get("trust_likely_duplicates", False)
    ) and not verify_by_hash
    recursive = bool(body.get("recursive", True))
    import_tags, location_from_gps, tag_options_err = (
        service._validate_import_tag_options(body)
    )
    if tag_options_err is not None:
        return tag_options_err

    carry_raw = body.get("carry_photo_ids")
    if carry_raw is None:
        carry_photo_ids = None
    else:
        if not isinstance(carry_raw, list) or not all(
            isinstance(x, int) and not isinstance(x, bool) and x > 0
            for x in carry_raw
        ):
            return ImportFailure(
                "carry_photo_ids must be a list of positive integers"
            )
        # A caller-supplied carry list must be bound to a real
        # parent import job. See parent_import_job_id above.
        if parent_allowed_ids is None:
            return ImportFailure(
                "carry_photo_ids requires parent_import_job_id "
                "(the failed import this retry inherits scope from)"
            )
        invalid = [pid for pid in carry_raw if pid not in parent_allowed_ids]
        if invalid:
            return ImportFailure(
                "carry_photo_ids contains IDs the parent import did "
                f"not land or inherit: {invalid[:5]}"
            )
        # Stable-identity re-check for each carried ID. ``photos.id``
        # is a bare ``INTEGER PRIMARY KEY`` — SQLite reuses freed IDs
        # on the next insert, so an ID that legitimately named one of
        # the parent's imports at parent-run time can, after a delete,
        # name an unrelated photo by retry-run time. Compare each
        # carried ID's CURRENT ``folder_path/filename`` against the
        # fingerprint recorded when the parent landed it; refuse a
        # mismatch rather than letting the after-import chain (and
        # any ``after_process_move``) sweep up the unrelated row.
        # Parents from before this fix carry no fingerprints at all
        # (empty dict), and are exempted — hard-rejecting every
        # legacy failed job's retry would be a bigger regression than
        # the narrow race window this protects.
        if parent_allowed_fingerprints:
            current_fingerprints = service._capture_photo_fingerprints_for_ids(
                db, carry_raw,
            )
            stale = []
            for pid in carry_raw:
                expected = parent_allowed_fingerprints.get(pid)
                if expected is None:
                    # Parent tracked fingerprints for some IDs but not
                    # this one — an ID the parent's ``allowed_ids``
                    # blessed but never fingerprinted (e.g. inherited
                    # from a legacy grandparent). Accept, matching
                    # the same rationale as the empty-dict case above.
                    continue
                current = current_fingerprints.get(pid)
                if current != expected:
                    stale.append(pid)
            if stale:
                return ImportFailure(
                    "carry_photo_ids no longer matches the files the "
                    "parent import landed (the numeric IDs now point "
                    "at different photos, likely because the parent's "
                    f"photos were deleted and re-imported): {stale[:5]}"
                )
        # Preserve caller order but deduplicate — the chain does not
        # need repeats and a giant duplicated list wastes work.
        seen_carry = set()
        carry_photo_ids = []
        for pid in carry_raw:
            if pid in seen_carry:
                continue
            seen_carry.add(pid)
            carry_photo_ids.append(pid)

    # A recovery retry must not silently import files the parent
    # never saw. Without this guard a retry against a source whose
    # contents changed since the failed run — a different SD card
    # mounted at the same path, or new photos added to the same
    # card — would silently enumerate every current file and copy
    # the newly-appeared ones, then flow their IDs into the
    # carried processing / NAS-move scope. The button says "Retry
    # failed files", not "Import whatever's at this path now".
    #
    # Each parent source's ``count`` + ``signature`` (sha256 over
    # the sorted ``(rel_path, size, mtime_ns)`` list) is captured
    # at parent DISCOVERY time in
    # ``import_job._capture_source_snapshots`` and persisted on
    # ``result["source_snapshots"]``. ``mtime_ns`` is in the tuple
    # so a same-size in-place replacement doesn't slip past the
    # size check; discovery-time capture keeps a card ejected
    # mid-copy from stamping ``-1`` sizes that would refuse a
    # legitimate reinsert-and-retry recovery. Here we recompute
    # the current signature for every retry source that shares a
    # path with a parent source and refuse the retry when any
    # signature has drifted. Legacy parents from before this fix
    # have no snapshots and fall through unchanged.
    if parent_source_snapshots:
        from import_job import _capture_source_snapshots
        from ingest import discover_source_files

        # Fail-closed on retry sources the parent never enumerated:
        # skipping validation for unknown paths would let a retry
        # import every file at that path (no prior catalog entry
        # gates them via ``skip_duplicates``) and stream those IDs
        # into the after-import chain / NAS-move scope, even though
        # the "Retry failed files" button only ever promised the
        # parent's failed files. See PR #1387 Codex review.
        parent_sources = {
            src for src, snapshot in parent_source_snapshots.items()
            if isinstance(snapshot, dict) and snapshot
        }
        requested_sources = set(sources)
        unknown_sources = sorted(requested_sources - parent_sources)
        if unknown_sources:
            unknown_source_text = ", ".join(unknown_sources[:3])
            return ImportFailure(
                "Retry submitted source paths the original import "
                "never enumerated (a different card mounted at the "
                "same path, or a new source added): "
                f"{unknown_source_text}. Start a new import instead "
                "of retrying."
            )
        missing_sources = sorted(parent_sources - requested_sources)
        if missing_sources:
            missing_source_text = ", ".join(missing_sources[:3])
            return ImportFailure(
                "A recovery retry must include every source from the "
                "original import. These sources are missing: "
                f"{missing_source_text}. Reconnect all original "
                "sources, or start a new import instead of retrying."
            )

        drifted_sources = []
        for src in sources:
            parent_snap = parent_source_snapshots.get(src)
            # Reuse the same discovery + snapshot helpers the parent
            # ran through so a mismatch here reflects a genuine
            # source change, not a difference in enumeration logic.
            try:
                current_files = discover_source_files(
                    src, file_types, recursive=recursive,
                )
            except Exception:
                log.exception(
                    "Failed to re-enumerate source for retry snapshot "
                    "check: %s", src,
                )
                drifted_sources.append(src)
                continue
            current_snap = _capture_source_snapshots(
                current_files, [src],
            ).get(src) or {}
            if current_snap.get("signature") != parent_snap.get(
                "signature",
            ):
                drifted_sources.append(src)
        if drifted_sources:
            return ImportFailure(
                "The source contents have changed since the original "
                "import (a different SD card at the same path, or new "
                "or missing files). Retrying would import files the "
                "original run never saw. Verify the source, then "
                "start a new import instead of retrying: "
                f"{drifted_sources[:3]}"
            )

    # A retry that keeps the same remote target must land on the
    # same host/root/mount as the failed run — otherwise the retry
    # would copy the remaining files to a different NAS, or (if
    # only the mount changed) skip prior successes based on the
    # old catalog view while transferring failures to a new
    # location. When the parent recorded a remote_target_snapshot,
    # verify that the current resolution matches; refuse the retry
    # if the target has been edited since. The Import-page and
    # Jobs-page retry helpers both send parent_import_job_id, so
    # both go through this check.
    if parent_config is not None:
        parent_snapshot = parent_config.get("remote_target_snapshot")
        parent_remote_target_id = parent_config.get("remote_target_id")
        if parent_snapshot is not None:
            current_snapshot = service._remote_target_snapshot(
                remote_archive_config,
            )
            if current_snapshot != parent_snapshot:
                return ImportFailure(
                    "The remote target for the original import has "
                    "changed (host, path, mount, or subpath differs). "
                    "Verify Settings → Remote targets and start a new "
                    "import instead of retrying."
                )
        elif parent_remote_target_id:
            # Parent job was a remote import from before the
            # snapshot check landed, so its persisted config
            # records only the target ID. Both retry helpers
            # reconstruct the request from that ID, and
            # ``_resolve_remote_archive_target`` then walks the
            # CURRENT Settings entry — a Settings edit since the
            # original run would silently redirect the transfer
            # to a different host/root/mount. Refuse the retry
            # instead of trusting the current resolution.
            return ImportFailure(
                "The original remote import predates the recovery-"
                "retry safety check (no remote-target snapshot was "
                "recorded). Verify Settings → Remote targets still "
                "point at the intended host, then start a new "
                "import instead of retrying."
            )

    # Snapshot the chosen saved process's stage flags at enqueue time
    # so a mid-import edit or delete can't silently change (or void)
    # the after-import run the user already accepted. An archive-copy
    # import from a full card can take many minutes, and until the
    # chain hook fires the pipeline_job's actual toggles are still up
    # for grabs — resolving here freezes them (mirrors the remote-
    # transport snapshot below).
    #
    # A recovery retry must inherit the parent's frozen snapshot when
    # the retry still points at the same process id: the original
    # enqueue already froze the stages the user accepted, and
    # resolving again would silently pick up any Settings edit made
    # after the failure (or fail outright if the process was
    # deleted). Re-resolve only when the retry deliberately switches
    # to a different process id — then the user is asking for
    # whatever that process currently is.
    after_import_snapshot = None
    if after_import is not None:
        reused_parent_snapshot = None
        if parent_config is not None:
            parent_after_import = parent_config.get("after_import")
            parent_snapshot_process = (
                parent_config.get("after_import_snapshot")
            )
            if (
                parent_snapshot_process is not None
                and parent_after_import == after_import
            ):
                reused_parent_snapshot = parent_snapshot_process
        if reused_parent_snapshot is not None:
            after_import_snapshot = reused_parent_snapshot
        else:
            try:
                after_import_snapshot = db.resolve_process(after_import)
            except ValueError as e:
                return ImportFailure(str(e), 404)

    # Validate the optional NAS move that chains after processing
    # completes. Requires after_import (the move fires from the process
    # job's completion hook) and a destination inside the target's
    # local archive root; snapshotted now so a mid-chain Settings edit
    # can't redirect the move. Runs before workspace creation so a bad
    # target/root/destination doesn't leave an orphan workspace behind.
    managed_staging = None
    parent_staging = (parent_config or {}).get("managed_staging")
    if parent_config is not None and bool(parent_config.get("defer_nas_transfer")) != defer_nas_transfer:
        return ImportFailure("A recovery retry must preserve the original NAS transfer timing")
    if bool(parent_staging) != local_processing and parent_config is not None:
        return ImportFailure("A recovery retry must preserve the original local processing choice")
    if local_processing:
        if after_import is None:
            return ImportFailure("Local processing requires an after-import process")
        from import_staging import plan_staged_import
        from pipeline_job import _load_known_mount_roots
        try:
            managed_staging, move_target_snapshot = plan_staged_import(
                os.path.dirname(service.config["THUMB_CACHE_DIR"]), destination,
                remote_archive_config, parent_staging, _load_known_mount_roots(db),
            )
            staging_real = os.path.realpath(managed_staging["destination"])
            if any(path_guard.contains_resolved(os.path.realpath(s), staging_real) for s in sources):
                return ImportFailure("Temporary processing storage cannot be inside a source directory")
        except (ValueError, OSError, RuntimeError) as e:
            return ImportFailure(str(e))
    else:
        move_target_snapshot, move_err = service._validate_after_process_move(
            after_process_move, after_import, destination, folder_template,
            file_types=file_types,
        )
        if move_err is not None:
            return move_err

    # A retry with a chained NAS move must land on the same
    # host/root/mount as the parent. When the parent recorded a
    # ``target_snapshot`` for the chained target, verify the
    # currently-resolved target matches — a Settings edit since
    # enqueue-time could otherwise silently redirect the chained
    # transfer even though the primary snapshot check above
    # accepted the request. When the parent had a chained move but
    # no snapshot (predates this check), refuse rather than trust
    # the current resolution — same reasoning as the primary
    # remote-target legacy check.
    if parent_config is not None:
        parent_move_cfg = parent_config.get("after_process_move") or {}
        parent_move_snapshot = parent_move_cfg.get("target_snapshot")
        parent_move_target_id = parent_move_cfg.get("remote_target_id")
        if parent_move_snapshot is not None:
            current_move_snapshot = service._move_target_snapshot(
                move_target_snapshot,
            )
            if current_move_snapshot != parent_move_snapshot:
                return ImportFailure(
                    "The chained NAS-move target for the original "
                    "import has changed (host, path, mount, or archive "
                    "root differs). Verify Settings → Remote targets "
                    "and start a new import instead of retrying."
                )
        elif parent_move_target_id:
            return ImportFailure(
                "The original import's chained NAS-move target "
                "predates the recovery-retry safety check (no target "
                "snapshot was recorded). Verify Settings → Remote "
                "targets still point at the intended host, then start "
                "a new import instead of retrying."
            )

    pending_archive_id = (
        os.path.basename(move_target_snapshot["managed_staging_root"]) if defer_nas_transfer else None
    )
    if pending_archive_id:
        completed = db.conn.execute(
            "SELECT 1 FROM pending_archives WHERE id = ? AND state = 'complete'", (pending_archive_id,),
        ).fetchone()
        if completed:
            return ImportFailure("These photos have already been sent to NAS. Start a new import instead of retrying.")

    active_ws, created_workspace, previous_active_ws, workspace_err = (
        service._prepare_import_workspace(db, body)
    )
    if workspace_err is not None:
        return workspace_err

    runner = service.get_runner()
    thumb_cache_dir = service.config["THUMB_CACHE_DIR"]
    vireo_dir = os.path.dirname(thumb_cache_dir)

    # Snapshot the resolved remote transport at enqueue time so a
    # settings edit between click-Start and job-run can't redirect the
    # archive to a different host/mount than the panel is showing
    # (mirrors the pipeline route's remote_target_snapshot).
    remote_target = None
    if remote_archive_config is not None and not local_processing:
        import move as move_mod

        spec = move_mod.build_remote_move_spec(
            remote_archive_config["target"],
            remote_archive_config["subpath"],
            rsync_bin,
        )
        remote_target = {
            "rsync_bin": rsync_bin,
            "remote": spec,
            "ssh_base": remote_archive_config["ssh_final"],
            "mount_base": remote_archive_config["mount_final"],
        }

    job_config = {
        "sources": sources,
        "destination": destination,
        "local_processing": local_processing,
        "managed_staging": managed_staging,
        "defer_nas_transfer": defer_nas_transfer,
        "pending_archive_id": pending_archive_id,
        "folder_template": folder_template,
        "file_types": file_types,
        "skip_duplicates": skip_duplicates,
        "verify_by_hash": verify_by_hash,
        "trust_likely_duplicates": trust_likely_duplicates,
        "recursive": recursive,
        "after_import": after_import,
        # Persist the enqueue-time snapshot alongside the process id so a
        # recovery retry can reuse the exact stages the user accepted.
        # Without this a retry silently re-resolves the current process
        # (an edit or delete between the failed run and the retry would
        # otherwise change or void what runs); see the reuse block above
        # and the corresponding remote-target snapshot handling.
        "after_import_snapshot": after_import_snapshot,
        "tags": import_tags,
        "location_from_gps": location_from_gps,
        "allow_missing_exiftool": bool(
            body.get("allow_missing_exiftool", False)
        ),
        "remote_target_id": remote_target_id or None,
        "remote_subpath": remote_subpath or None,
        "remote_target_snapshot": service._remote_target_snapshot(
            remote_archive_config,
        ),
        "workspace_id": active_ws,
        "created_workspace": created_workspace,
        "carry_photo_ids": carry_photo_ids,
        # Resume of an interrupted parent only (see
        # ``_validate_parent_import_job``): the paths its landings sit at,
        # so this run recovers rows the parent's checkpoint missed, and the
        # photos still owed the tag/GPS pass. Persisted so a resume of
        # this run, should it be interrupted too, inherits both.
        "recover_landed_files": (
            parent_resume["landed_files"] if parent_resume else {}
        ),
        "untagged_photo_ids": (
            parent_resume["untagged_ids"] if parent_resume else []
        ),
        # Fingerprint sidecar to carry_photo_ids so a retry-of-retry
        # can still verify the inherited scope by stable identity
        # even after the grandparent's job has aged out of history.
        # Keyed by str(id) for JSON round-tripping; empty for
        # first-attempt imports or legacy parents with no
        # fingerprints of their own.
        "carry_photo_fingerprints": (
            {
                str(pid): parent_allowed_fingerprints[pid]
                for pid in (carry_photo_ids or [])
                if parent_allowed_fingerprints
                and pid in parent_allowed_fingerprints
            }
            if parent_allowed_fingerprints
            else {}
        ),
        # Persist the parent's id so the Jobs page's parallel-retry
        # gate (``hasActiveRetryFor``) can recognize this retry as
        # belonging to that parent and suppress a second Retry button
        # click while this run is still in flight. Without the field
        # on ``job_config`` the gate reads ``undefined`` on every
        # active job, so reselecting the failed parent renders a
        # live Retry button that would race the in-flight retry.
        "parent_import_job_id": (
            parent_id_raw.strip() if parent_id_raw else None
        ),
        # Root of the retry chain: the original failed import that
        # every retry (and retry-of-retry) descends from. Persisted
        # separately from ``parent_import_job_id`` so the Jobs page
        # can gate parallel launches on the original selection even
        # when the active job's direct parent is another retry.
        # Without this a retry-of-retry's ``parent_import_job_id``
        # points at the first retry, so reselecting the original
        # failed import still renders a live Retry button that would
        # race the in-flight retry. Inherits the parent's root when
        # the parent already carries one; otherwise the parent IS
        # the root of this chain.
        "root_import_job_id": (
            (parent_config.get("root_import_job_id")
             or parent_id_raw.strip())
            if parent_config is not None and parent_id_raw
            else None
        ),
    }
    if include_paths is not None:
        job_config["previewed_count"] = previewed_count
        job_config["checked_count"] = checked_count
        # Persist the actual path list so a recovery retry can
        # reconstruct the original selection. Without this a
        # ``retryBodyFromFinishedJob``-driven retry would either be
        # rejected by the source-signature drift check (parent
        # snapshot is now over the pre-selection set — see
        # ``_capture_source_snapshots`` — but the ``include_paths``
        # the parent actually ran is the useful thing to compare
        # against a retry that also filters) or, once that hurdle is
        # cleared, silently re-import the files the user deliberately
        # deselected. Stored as a sorted list so the JSON round-trips
        # deterministically; the set is rebuilt in ``_apply_selection``.
        # Size cost is bounded by ``previewed_count`` (thousands of
        # short path strings in the worst realistic case) and is
        # accepted deliberately as the price of a retry that stays
        # true to the parent's scope.
        job_config["include_paths"] = sorted(include_paths)
    if move_target_snapshot is not None and not local_processing:
        job_config["after_process_move"] = {
            "remote_target_id": move_target_snapshot["id"],
            "target_name": move_target_snapshot["name"],
            # Persisted alongside the id so a recovery retry can
            # detect a Settings edit that would redirect the
            # chained transfer to a different host or root.
            # Parallels ``remote_target_snapshot`` for the primary
            # remote destination. See the parent-verification
            # block above.
            "target_snapshot": service._move_target_snapshot(
                move_target_snapshot,
            ),
        }

    def _chain_after_import(job, result):
        """Create the import collection and optionally enqueue processing.

        Every skip is written to the result as ``after_import_skipped``
        so the jobs panel shows exactly why processing did not run.  The
        collection is independent: every successful import with new
        photos gets one, including the import-only choice.
        """
        photo_ids = result.get("photo_ids") or []
        carry_photo_ids = list(
            (job.get("config") or {}).get("carry_photo_ids") or []
        )
        carried_already = set(carry_photo_ids)
        carry_photo_ids += [
            pid for pid in result.get("recovered_photo_ids") or []
            if pid not in carried_already
        ]
        thread_db, col_id = service._record_import_collection(
            result, active_ws, chain_photo_ids=carry_photo_ids,
        )
        if pending_archive_id:
            if thread_db is not None:
                thread_db.conn.execute(
                    "UPDATE pending_archives SET collection_id = COALESCE(?, collection_id) WHERE id = ?",
                    (col_id, pending_archive_id),
                )
                thread_db.conn.commit()
            result["nas_transfer_deferred"] = True
            result["pending_archive_id"] = pending_archive_id
        # Recovery-retry imports may carry forward files earlier
        # attempts landed. The original failed run skipped its
        # after-import chain because ``ok`` was False, so those files
        # were never processed. Roll them into the chain scope now so
        # the collection AND the folder-based after-move both cover
        # the complete original import, not just the newly-recovered
        # files. ``carried_photo_ids`` on the result is the validated
        # subset actually included, so a stale ID never leaks into
        # this scope.
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
            result["after_import_skipped"] = "no new photos"
            return
        if col_id is None:
            result["after_import_skipped"] = (
                "failed to create import collection"
            )
            return
        try:
            # Which imported folders must the chained NAS move relocate?
            # Computed here — not at request time — because the archive
            # folder rows only exist once the copy has landed. Minimal
            # non-nested set: moving an ancestor also moves its
            # descendants. The target itself is the enqueue-time
            # snapshot, so a Settings edit mid-chain can't redirect the
            # move. Uses ``chain_scope`` so a recovery retry moves the
            # folders holding the original run's successful files too.
            after_move = None
            if move_target_snapshot is not None and not defer_nas_transfer:
                from import_chain import minimal_move_set
                folder_rows = []
                for i in range(0, len(chain_scope), 500):
                    chunk = chain_scope[i:i + 500]
                    ph = ",".join("?" * len(chunk))
                    folder_rows.extend(thread_db.conn.execute(
                        "SELECT DISTINCT f.id, f.path FROM photos p "
                        "JOIN folders f ON f.id = p.folder_id "
                        f"WHERE p.id IN ({ph})", chunk).fetchall())
                root = move_target_snapshot["local_archive_root"]
                moves, move_skips = minimal_move_set(
                    root, [(r["id"], r["path"]) for r in folder_rows])
                after_move = {
                    "target": move_target_snapshot,
                    "folders": moves,
                }
                if move_skips:
                    # Importing straight into the archive root with a
                    # template that renders empty catalogs photos ON
                    # the root folder itself; minimal_move_set refuses
                    # to move the root (it would sweep unrelated
                    # shoots into the transfer). Say so instead of a
                    # bare "no folders to move" — the user accepted a
                    # chain that ends on the NAS, and these photos
                    # won't get there.
                    prefix = "photos" if not moves else "some photos"
                    if any(s["reason"] == "root" for s in move_skips):
                        after_move["skip_note"] = (
                            prefix + " landed directly in the archive "
                            "root — moving the root would sweep "
                            "unrelated shoots into the transfer, so "
                            "they stay local; move them from the Move "
                            "page")
                    else:
                        after_move["skip_note"] = (
                            prefix + " landed outside the archive "
                            "root, so they stay local; move them from "
                            "the Move page")
            process_job_id, model_warning, process_blocker = (
                service.enqueue_process_job(
                    thread_db, runner, active_ws,
                    collection_id=col_id,
                    process_id=after_import,
                    chained_from=job["id"],
                    expanded=after_import_snapshot,
                    after_move=after_move,
                )
            )
            if process_blocker:
                result["after_import_skipped"] = process_blocker
                if after_move:
                    # Processing is paused before any pipeline job was
                    # enqueued (e.g. Classify needs a species list the
                    # user hasn't downloaded yet), so the finally-hook
                    # that normally fires the chained NAS move never
                    # runs. But the user accepted a chain that ends on
                    # the NAS — leaving these photos in the local
                    # archive without saying so would break the promise
                    # and hide the outcome behind an "after_import
                    # skipped" pill that doesn't mention the move. Fire
                    # the move here off the import job itself, so the
                    # "photos end on the NAS" invariant holds the same
                    # way it does for a runtime process failure. The
                    # hook writes move_job_ids / after_move_errors on
                    # the import result and adds a "Move to NAS" step
                    # to the import job's tree, so the outcome is
                    # visible either way.
                    service.chain_after_move(
                        job, result, after_move, active_ws)
                return
            result["process_job_id"] = process_job_id
            if after_move is not None:
                # Surface the planned move on the import's result card so
                # the user can see what will happen before it fires —
                # including, honestly, that nothing (or not everything)
                # will move when folders were skipped.
                result["after_process_move_planned"] = {
                    "target_name": move_target_snapshot["name"],
                    "folders": after_move["folders"],
                }
                if after_move.get("skip_note"):
                    result["after_process_move_planned"]["note"] = (
                        after_move["skip_note"])
            if model_warning:
                result["model_warning"] = model_warning
        except Exception as e:
            # The import itself succeeded — record the chaining failure
            # rather than flipping the whole job red, but never
            # silently: the user asked for processing and must see it
            # didn't start.
            log.exception("after-import chaining failed")
            result["after_import_skipped"] = (
                f"failed to enqueue processing: {e}"
            )

    def _mark_post_import_step(job, step, result=None):
        """Record a finished post-import step on the history row now.

        A restart before the runner writes the final result leaves the row
        marked interrupted; these marks tell a resume which side effects
        already happened (``_interrupted_parent_resume``) so it doesn't
        tag, collect or chain the same photos twice. ``result``, once the
        run is otherwise done, rides along so the row still carries what
        an ordinary retry needs (``failed`` and the rest).
        """
        job["partial_result"] = {
            **(job.get("partial_result") or {}), **(result or {}), step: True,
        }
        runner.flush_partial_result(job)

    def work(job):
        from import_job import ImportParams, run_import_job

        import_destination = destination
        if managed_staging:
            from local_processing import selected_source_files, storage_plan, total_file_bytes
            import_destination = managed_staging["destination"]
            files = selected_source_files(sources, file_types, recursive)
            if include_paths is not None:
                files = [p for p in files if str(p) in include_paths]
            space = storage_plan(vireo_dir, total_file_bytes(files))
            if not space["enough"]:
                raise ValueError("Not enough free space on this computer for temporary processing. Free up space or import fewer photos.")
            os.makedirs(import_destination, exist_ok=True)
            if pending_archive_id:
                from pending_archives import register_pending_archive
                with Database(service.db_path) as pending_db:
                    pending_db.set_active_workspace(active_ws)
                    register_pending_archive(
                        pending_db, pending_archive_id, destination, import_destination, move_target_snapshot,
                    )

        params = ImportParams(
            sources=sources,
            destination=import_destination,
            folder_template=folder_template,
            file_types=file_types,
            skip_duplicates=skip_duplicates,
            verify_by_hash=verify_by_hash,
            trust_likely_duplicates=trust_likely_duplicates,
            recursive=recursive,
            after_import=after_import,
            remote_target=remote_target,
            vireo_dir=vireo_dir,
            thumb_cache_dir=thumb_cache_dir,
            include_paths=include_paths,
            previewed_count=previewed_count,
            checked_count=checked_count,
            carry_photo_ids=carry_photo_ids,
            recover_landed_files=(
                parent_resume["landed_files"] if parent_resume else None
            ),
        )
        try:
            result = run_import_job(
                job, runner, service.db_path, active_ws, params,
            )
            if managed_staging:
                result["local_processing"] = True
                result["final_destination"] = destination
                result["staging_destination"] = import_destination
            # An interrupted parent never reached its tag/GPS pass, so a
            # resume applies it to the photos that parent landed (its
            # checkpointed ids, inherited untagged ids, and rows this run
            # recovered by path) along with its own. Every other carried
            # photo was tagged and located by the run that landed it;
            # re-resolving GPS for those would overwrite any location the
            # user has corrected since.
            carried = set(carry_photo_ids or ())
            # With duplicate skipping off, the parent's landings come back
            # as this run's adoptions; they are tagged only if owed.
            parent_landings = set(result.get("parent_landing_ids") or [])
            tag_photo_ids = [
                pid for pid in result.get("photo_ids") or []
                if pid not in parent_landings
            ]
            seen = set(tag_photo_ids)
            owed = [
                pid for pid in (parent_resume or {}).get("untagged_ids", [])
                if pid in carried
            ]
            if parent_resume and not parent_resume["tags_applied"]:
                owed += sorted(parent_landings)
            for pid in owed:
                if pid not in seen:
                    seen.add(pid)
                    tag_photo_ids.append(pid)
            service._apply_import_tags(
                active_ws, tag_photo_ids, import_tags,
                location_from_gps, result, job=job, runner=runner,
            )
            # A pass Stop cut short, or one with failed tags or locations,
            # still owes work; leave it unmarked so a resume replays it.
            if not result.get("cancelled") and not (
                (result.get("tagging") or {}).get("errors")
            ):
                _mark_post_import_step(job, "tags_applied")
            # Atomically honor a pending pause/cancel before collection
            # publication and child-job handoff. The shared runner gate
            # rejects new requests once this final phase begins.
            if not runner.begin_uncancellable(job["id"]):
                result["cancelled"] = True
            _chain_after_import(job, result)
            # A cancelled run skipped the chain (and may owe tags), so it
            # stays resumable; only a chain that ran is marked.
            if not result.get("cancelled"):
                _mark_post_import_step(job, "chained", result)
            return result
        finally:
            # run_import_job can flip destination folders from
            # ``missing`` to ``ok`` and re-scans landed files, so a
            # ready /api/photos/missing cache computed before the
            # import can now list rows whose originals are back on
            # disk. The other scan/import paths (rescan-this-folder,
            # import-in-place) already invalidate the cache after
            # they touch disk; do the same here so the banner/modal
            # stop offering ghosts for photos this job just restored,
            # even if the job failed part-way (rows land
            # incrementally). Best-effort: never let a cache-drop
            # failure mask the underlying import result.
            try:
                service.invalidate_missing_originals()
            except Exception:
                log.exception(
                    "Failed to invalidate missing-originals cache "
                    "after import-photos job",
                )

    # Server-side retry-exclusivity gate: refuse a retry whose
    # root ancestor already has an active retry in flight, even
    # when the Jobs-page UI gate was bypassed (two tabs, a stale
    # UI that didn't observe the sibling retry yet, or a direct
    # API caller). Without this an overlapping retry can enqueue
    # a second import against the same source and destination and
    # race the chained after-import / NAS move against the first
    # one. Runs after all snapshot validation so a rejected
    # request costs no worker thread. Best-effort against
    # exact-simultaneous starts — ``list_jobs`` releases the
    # runner lock before ``start`` takes it — which is acceptable
    # for the double-click / two-tab case this fix targets; the
    # follow-up work is to fold both under one runner-side call.
    # See PR #1387 Codex review.
    retry_root = job_config.get("root_import_job_id")
    if retry_root:
        for other in runner.list_jobs():
            if other.get("type") != "import":
                continue
            # ``pausing`` is a live status: ``pause_job`` publishes
            # it immediately, but the worker keeps running until it
            # reaches ``is_cancelled`` and only then flips to
            # ``paused``. Treating ``pausing`` as inactive would let a
            # second retry slip in during that window and race the
            # first one's chained processing / NAS move.
            if other.get("status") not in (
                "queued", "running", "pausing", "paused",
            ):
                continue
            other_cfg = other.get("config") or {}
            other_root = (
                other_cfg.get("root_import_job_id")
                or other_cfg.get("parent_import_job_id")
            )
            # Only reject a competing RETRY — one whose own
            # ancestry root matches ours. The failed parent itself
            # (still in ``_jobs`` briefly after finishing) has no
            # ancestry field so it never matches here, letting the
            # first retry through. The parent's own liveness is
            # gated separately by ``_validate_parent_import_job``.
            if other_root and other_root == retry_root:
                return ImportFailure(
                    "Another retry for the same failed import is "
                    "already in flight; wait for it to finish "
                    "before starting a new one.",
                    409,
                )

    # Re-check ``local_copy_scan_conflict`` and register the runner job
    # atomically under ``stage_boundary_lock``. A folder-stage request
    # that arrives after the earlier pre-flight release but before this
    # registration would otherwise see no import job and be admitted,
    # letting the stage rebase the destination this import is about to
    # copy into and scan.
    with stage_boundary_lock():
        pending_sources = stage_pending_source_paths(
            runner.list_jobs if runner is not None else None,
            db,
        )
        conflict = local_copy_scan_conflict(
            db, _import_photos_conflict_paths,
            active_workspace_id=db._active_workspace_id,
            pending_stage_sources=pending_sources,
        )
        if conflict:
            # A ``new_workspace_name`` request has already committed the
            # workspace and switched active to it. Roll both back so the
            # 409 leaves no orphan and no silent active-workspace change.
            service._rollback_import_workspace(
                db, created_workspace, previous_active_ws,
            )
            return ImportFailure(conflict, 409)
        job_id = runner.start(
            "import", work, config=job_config, workspace_id=active_ws,
            pausable=True,
        )
    response = {"job_id": job_id}
    if created_workspace is not None:
        response["workspace"] = created_workspace
    return response
