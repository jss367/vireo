"""Durable originals awaiting the user's decision to send them to the NAS.

These records do not expire with job history or preview caches. The source
directory is exclusive to one import (including its recovery retries).
"""

import contextlib
import json
import os

from path_guard import contains_resolved
from services import workspace_folders

LIVE_STATUSES = {"running", "queued", "pausing", "paused"}


def register_pending_archive(db, archive_id, destination, staging_destination, target):
    with db.transaction():
        db.pending_archives.register(archive_id, destination, staging_destination, target)


def get_pending_archive(db, archive_id):
    row = db.pending_archives.get(archive_id)
    return dict(row) if row else None


def active_archive_jobs(runner, workspace_id):
    # A move must wait for processing, exports, deletions, and other writers.
    # Jobs that touch no photo or folder path opt out through
    # blocks_local_transitions (jobs.CATALOG_INDEPENDENT_JOB_TYPES).
    return [j for j in runner.list_jobs()
            if j.get("workspace_id") == workspace_id
            and j.get("status") in LIVE_STATUSES
            and j.get("blocks_local_transitions", True)]


def _check_archive_mount_confirmation(identities, confirmed_identities):
    """A caller's explicit confirmation must match saved evidence and the live mount.

    Matching only the live snapshot is not enough: another tab may have recorded
    different identities between proposal and submission, and the mount may have
    since reverted to the one named on the original prompt. Reject when the
    confirmation names an identity that already differs from what is saved, so
    the stale tab never dispatches a transfer that the worker would reject.
    """
    from import_staging import _archive_mount_identities

    if (not isinstance(confirmed_identities, dict) or not confirmed_identities
            or not confirmed_identities.keys() <= identities.keys()):
        raise ValueError("The NAS destination changed since confirmation. Try Send to NAS again to review it.")
    saved = json.loads(json.dumps({root: identities[root] for root in confirmed_identities}))
    if confirmed_identities != saved:
        raise ValueError("The NAS destination changed since confirmation. Try Send to NAS again to review it.")
    current = _archive_mount_identities({root: True for root in confirmed_identities})
    if (any(identity is None for identity in current.values())
            or confirmed_identities != json.loads(json.dumps(current))):
        raise ValueError("The NAS destination changed since confirmation. Try Send to NAS again to review it.")


def prepare_archive_mount(db, archive, confirmed_identities=None):
    """Offer explicit recovery of missing evidence, never replace known evidence.

    Confirmation is bound to the live identities shown to the caller. The
    transfer will check the persisted identities again before copying and
    before committing the move.
    """
    from import_staging import _archive_mount_identities, check_staged_mount

    target = json.loads(archive["target_json"])
    if target.get("transport") != "mounted":
        return None
    identities = target.get("mount_identities") or {}
    missing = [root for root, identity in identities.items() if identity is None]
    if not missing:
        if confirmed_identities is not None:
            _check_archive_mount_confirmation(identities, confirmed_identities)
        return None
    baseline = target.get("mount_baseline") or {}
    current = _archive_mount_identities({root: True for root in missing})
    proposed = {**identities, **current}
    # Check availability and all known identities before offering recovery.
    check_staged_mount(archive["destination"], baseline, proposed)
    proposal = json.loads(json.dumps(current))
    if confirmed_identities is None:
        return proposal
    if confirmed_identities != proposal:
        raise ValueError("The NAS destination changed since confirmation. Try Send to NAS again to review it.")
    target["mount_identities"] = proposed
    with db.transaction():
        db.pending_archives.update_target(archive["id"], target)
    archive["target_json"] = json.dumps(target)
    return None


def send_pending_archive(db, archive, *, vireo_dir, guard_folder, progress_cb, control=None, verification_cb=None):
    """Move the remaining directory through the verified move implementation.

    The saved target is authoritative; never resolve the destination from
    today's Settings or from a caller-supplied path. Move the whole import
    root so retained originals, sidecars, and published files travel together.
    """
    import config
    import move
    from import_staging import check_staged_mount

    source = archive["staging_destination"]
    target = json.loads(archive["target_json"])
    root = target["managed_staging_root"]
    root_real, source_real = os.path.realpath(root), os.path.realpath(source)
    if (not contains_resolved(os.path.realpath(os.path.join(vireo_dir, "staging")), root_real)
            or not contains_resolved(root_real, source_real)
            or os.path.realpath(os.path.dirname(source)) != root_real
            or source_real == root_real):
        raise ValueError("The local originals no longer match their saved location.")
    if not os.path.isdir(source):
        raise ValueError("Local originals are unavailable. Reconnect their storage before sending to NAS.")
    folder_id = workspace_folders.add_folder(db, source)
    error = guard_folder(db, folder_id)
    if error:
        raise ValueError(error)
    destination = target["mount_path"]
    remote = None
    check_mount = None
    if target.get("transport") == "mounted":
        def check_mount():
            check_staged_mount(archive["destination"], target.get("mount_baseline"), target.get("mount_identities"))
        check_mount()
    else:
        effective = db.get_effective_config(config.load())
        rsync = move.resolve_rsync_bin(effective.get("rsync_bin", "") or "")
        ssh = move.resolve_ssh_bin(effective.get("ssh_bin", "") or "")
        if not rsync or not move.is_gnu_rsync(rsync) or not ssh:
            raise ValueError("GNU rsync and OpenSSH are required to send to this NAS target.")
        remote = move.build_remote_move_spec(target, "", rsync, ssh)
    result = move.move_folder(
        db, folder_id, destination, progress_cb=progress_cb,
        developed_dir=db.get_effective_config(config.load()).get("darktable_output_dir", "") or "",
        merge=True, remote=remote, allow_tracked_merge=True,
        verify_contents=remote is None,
        **({"verification_cb": verification_cb} if verification_cb else {}),
        thumb_cache_dir=os.path.join(vireo_dir, "thumbnails"),
        **({"control": control} if control else {}),
        **({"pre_commit_check": check_mount} if check_mount else {}),
    )
    if result.get("errors") or result.get("needs_merge"):
        raise ValueError("; ".join(result.get("errors") or ["The NAS transfer needs attention."]))
    # No rmtree: only verified move_folder may delete originals. Empty import
    # parents can be removed after successful catalog publication.
    with contextlib.suppress(OSError):
        os.rmdir(root)
    summary = "Remaining photos sent to NAS"
    if result.get("cleanup_error"):
        summary += f"; local cleanup needs attention at {source}: {result['cleanup_error']}"
    return {**result, "ok": True, "summary": summary}
