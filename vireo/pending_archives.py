"""Durable originals awaiting the user's decision to send them to the NAS.

These records do not expire with job history or preview caches. The source
directory is exclusive to one import (including its recovery retries).
"""

import contextlib
import json
import os

from path_guard import contains_resolved

LIVE_STATUSES = {"running", "queued", "pausing", "paused"}


def register_pending_archive(db, archive_id, destination, staging_destination, target):
    db.conn.execute(
        "INSERT OR IGNORE INTO pending_archives "
        "(id, workspace_id, destination, staging_destination, target_json) VALUES (?, ?, ?, ?, ?)",
        (archive_id, db._ws_id(), destination, staging_destination, json.dumps(target)),
    )
    db.conn.commit()


def get_pending_archive(db, archive_id):
    row = db.conn.execute(
        "SELECT * FROM pending_archives WHERE id = ? AND workspace_id = ?",
        (archive_id, db._ws_id()),
    ).fetchone()
    return dict(row) if row else None


def active_archive_jobs(runner, workspace_id):
    # A move must wait for processing, exports, deletions, and other writers.
    # Observational jobs opt out through blocks_local_transitions.
    return [j for j in runner.list_jobs()
            if j.get("workspace_id") == workspace_id
            and j.get("status") in LIVE_STATUSES
            and j.get("blocks_local_transitions", True)]


def send_pending_archive(db, archive, *, vireo_dir, guard_folder, progress_cb):
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
    folder_id = db.add_folder(source)
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
