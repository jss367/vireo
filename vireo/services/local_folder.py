"""Shared, folder-scoped managed local copies.

The folder catalog is global while workspaces are views over that catalog.
Local residency therefore belongs to a top-level folder, not to a workspace:
every workspace linked to the folder sees the same rebased catalog paths and
the same managed copy.  Workspace-level actions are implemented as bulk
operations over these folder sessions.

The original workspace-scoped implementation remains available for recovery
of sessions created by Vireo 0.24.0.  New sessions are created here.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import threading
import time
from contextlib import suppress
from pathlib import Path

from services.local_workspace import (
    MANIFEST_VERSION,
    LocalWorkspaceCancelled,
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    _ancestor_conflicts,
    _atomic_publish,
    _change_summary,
    _collect_source_entries,
    _copy_entry,
    _entry_type,
    _is_within,
    _local_changes,
    _managed_root_state,
    _matches_remote,
    _physical_is_within,
    _prepare_publish_target,
    _relative,
    _source_state,
    _walk_entries,
    _write_manifest,
    stage_boundary_lock,
)

_LOCKS: dict[int, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


def _folder_lock(root_folder_id: int) -> threading.RLock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(int(root_folder_id), threading.RLock())


def folder_dir(vireo_dir: str, root_folder_id: int) -> Path:
    return Path(vireo_dir) / "local-folders" / str(int(root_folder_id))


def manifest_path(vireo_dir: str, root_folder_id: int) -> Path:
    return folder_dir(vireo_dir, root_folder_id) / "manifest.json"


def _sync_recovery_path(vireo_dir: str, root_folder_id: int) -> Path:
    return folder_dir(vireo_dir, root_folder_id) / "sync-recovery.json"


def _remove_folder_dir(vireo_dir: str, root_folder_id: int) -> None:
    """Remove a managed tree without ever following a replacement symlink."""
    base = folder_dir(vireo_dir, root_folder_id)
    try:
        st = os.lstat(base)
    except FileNotFoundError:
        return
    if stat.S_ISLNK(st.st_mode):
        with suppress(FileNotFoundError):
            os.unlink(base)
    elif stat.S_ISDIR(st.st_mode):
        shutil.rmtree(base, ignore_errors=True)
    else:
        with suppress(FileNotFoundError):
            os.unlink(base)


def folder_state(db, root_folder_id: int) -> dict | None:
    row = db.conn.execute(
        """SELECT root_folder_id, state, created_at, activated_at
           FROM local_folders WHERE root_folder_id=?""",
        (root_folder_id,),
    ).fetchone()
    return dict(row) if row else None


def _mappings(db, root_folder_id: int) -> list[dict]:
    rows = db.conn.execute(
        """SELECT root_folder_id, folder_id, source_path, local_path,
                  original_status, is_root
           FROM local_folder_mappings
           WHERE root_folder_id=?
           ORDER BY is_root DESC, source_path""",
        (root_folder_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _root_mapping(db, root_folder_id: int) -> dict | None:
    row = db.conn.execute(
        """SELECT root_folder_id, folder_id, source_path, local_path,
                  original_status, is_root
           FROM local_folder_mappings
           WHERE root_folder_id=? AND is_root=1""",
        (root_folder_id,),
    ).fetchone()
    return dict(row) if row else None


def local_root_for_folder(db, folder_id: int) -> int | None:
    """Return the local-session root covering ``folder_id``, if any."""
    row = db.conn.execute(
        "SELECT root_folder_id FROM local_folder_mappings WHERE folder_id=?",
        (folder_id,),
    ).fetchone()
    return int(row["root_folder_id"]) if row else None


def local_root_under_folder(db, folder_id: int) -> int | None:
    """Return a local-session root whose source lives inside ``folder_id``.

    Staging rebases the child folder row's ``folders.path`` under
    ``local-folders/``, so a subtree scan on ``folders.path`` (as
    ``delete_folder``/``relocate_folder`` do) no longer sees it — while
    ``local_folder_mappings.source_path`` still records the original
    location beneath the ancestor. Folder-level mutations must consult
    the recorded ``source_path`` so ancestor deletes/relocates refuse
    with 409 instead of tripping the parent_id FK when the row is gone.
    """
    row = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (folder_id,)
    ).fetchone()
    if row is None or not row["path"]:
        return None
    folder_path = row["path"]
    for entry in db.conn.execute(
        "SELECT root_folder_id, source_path FROM local_folder_mappings WHERE is_root=1"
    ).fetchall():
        source = entry["source_path"]
        if not source or source == folder_path:
            continue
        if _is_within(source, folder_path):
            return int(entry["root_folder_id"])
    return None


def folder_has_local_copy(db, folder_id: int) -> bool:
    return local_root_for_folder(db, folder_id) is not None


def workspace_local_root_ids(db, workspace_id: int) -> list[int]:
    rows = db.conn.execute(
        """SELECT DISTINCT lfm.root_folder_id
           FROM workspace_folders wf
           JOIN local_folder_mappings lfm ON lfm.folder_id = wf.folder_id
           WHERE wf.workspace_id=?
           ORDER BY lfm.root_folder_id""",
        (workspace_id,),
    ).fetchall()
    return [int(row["root_folder_id"]) for row in rows]


def workspace_has_local_folders(db, workspace_id: int) -> bool:
    return bool(workspace_local_root_ids(db, workspace_id))


def affected_workspace_ids(db, root_folder_id: int) -> list[int]:
    rows = db.conn.execute(
        """SELECT DISTINCT wf.workspace_id
           FROM local_folder_mappings lfm
           JOIN workspace_folders wf ON wf.folder_id = lfm.folder_id
           WHERE lfm.root_folder_id=?
           ORDER BY wf.workspace_id""",
        (root_folder_id,),
    ).fetchall()
    return [int(row["workspace_id"]) for row in rows]


def workspace_ids_for_folder_tree(db, root_folder_id: int) -> list[int]:
    """Return every workspace whose catalog scope intersects this root.

    Unlike :func:`affected_workspace_ids`, this also works before the local
    mapping rows are inserted, so stage validation can see jobs running in a
    different workspace that shares the source folder.
    """
    root = db.conn.execute("SELECT path FROM folders WHERE id=?", (root_folder_id,)).fetchone()
    if root is None:
        return []
    workspace_ids = set()
    rows = db.conn.execute(
        """SELECT wf.workspace_id, f.path
           FROM workspace_folders wf
           JOIN folders f ON f.id = wf.folder_id"""
    ).fetchall()
    for row in rows:
        if _is_within(row["path"], root["path"]):
            workspace_ids.add(int(row["workspace_id"]))
    return sorted(workspace_ids)


def _load_manifest(vireo_dir: str, root_folder_id: int) -> dict | None:
    path = manifest_path(vireo_dir, root_folder_id)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LocalWorkspaceError(f"Local folder manifest is unreadable: {exc}") from exc
    if data.get("version") != MANIFEST_VERSION:
        raise LocalWorkspaceError("Local folder manifest was created by an unsupported Vireo version")
    if int(data.get("root_folder_id", -1)) != int(root_folder_id):
        raise LocalWorkspaceError("Local folder manifest belongs to another folder")
    return data


def _write_sync_recovery(vireo_dir: str, root_folder_id: int, deleted_keys) -> None:
    _write_manifest(
        _sync_recovery_path(vireo_dir, root_folder_id),
        {"confirmed_deletions": [[int(index), rel] for index, rel in deleted_keys]},
    )


def _load_sync_recovery(vireo_dir: str, root_folder_id: int) -> set | None:
    path = _sync_recovery_path(vireo_dir, root_folder_id)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LocalWorkspaceError(f"Sync recovery marker is unreadable: {exc}") from exc
    result = set()
    for entry in data.get("confirmed_deletions", []):
        if isinstance(entry, list) and len(entry) == 2:
            with suppress(TypeError, ValueError):
                result.add((int(entry[0]), str(entry[1])))
    return result


def _catalog_records(db, root_folder_id: int, local_base: Path) -> tuple[list[dict], list[dict]]:
    row = db.conn.execute(
        "SELECT id, path, status FROM folders WHERE id=?", (root_folder_id,)
    ).fetchone()
    if row is None:
        raise LocalWorkspaceError("Folder not found")
    source_path = row["path"]

    # A folder already covered by a broader local root simply shares that
    # copy; callers should use the covering session instead of creating an
    # overlapping tree.  A requested broader root over an existing narrower
    # session must be resolved first because two manifests cannot safely own
    # the same catalog rows.
    for existing in db.conn.execute(
        "SELECT root_folder_id, source_path FROM local_folder_mappings WHERE is_root=1"
    ).fetchall():
        if _is_within(source_path, existing["source_path"]) or _is_within(
            existing["source_path"], source_path
        ):
            raise LocalWorkspaceError(
                f"Folder overlaps an existing local copy: {existing['source_path']}"
            )

    # Do not overlap a legacy v0.24 workspace session. It remains usable for
    # sync/discard, but new folder sessions wait until it is resolved.
    for existing in db.conn.execute(
        "SELECT workspace_id, source_path FROM local_workspace_folders WHERE is_root=1"
    ).fetchall():
        if _is_within(source_path, existing["source_path"]) or _is_within(
            existing["source_path"], source_path
        ):
            raise LocalWorkspaceError(
                "Folder is part of an older workspace-local session. "
                "Finish or discard that session before staging this folder."
            )

    name = Path(source_path.rstrip("/\\")).name or f"folder-{root_folder_id}"
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
    local_root = local_base / safe_name
    if _physical_is_within(str(local_root), source_path) or _physical_is_within(
        source_path, str(local_root)
    ):
        raise LocalWorkspaceError(
            "Managed local storage overlaps the source folder; move Vireo's data directory to local storage first"
        )

    root = {
        "folder_id": int(row["id"]),
        "source_path": source_path,
        "local_path": str(local_root),
    }
    folders = []
    for folder in db.conn.execute("SELECT id, path, status FROM folders ORDER BY path").fetchall():
        if not _is_within(folder["path"], source_path):
            continue
        folders.append(
            {
                "folder_id": int(folder["id"]),
                "source_path": folder["path"],
                "local_path": os.path.normpath(
                    os.path.join(str(local_root), _relative(folder["path"], source_path))
                ),
                "status": folder["status"],
                "is_root": int(folder["id"]) == int(root_folder_id),
            }
        )
    return [root], folders


def _delete_state_rows(db, root_folder_id: int) -> None:
    db.conn.execute("DELETE FROM local_folder_mappings WHERE root_folder_id=?", (root_folder_id,))
    db.conn.execute("DELETE FROM local_folders WHERE root_folder_id=?", (root_folder_id,))


def stage_folder(
    db,
    root_folder_id: int,
    vireo_dir: str,
    *,
    progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Copy one top-level folder locally and atomically rebase the catalog."""
    root_folder_id = int(root_folder_id)
    with _folder_lock(root_folder_id):
        base = folder_dir(vireo_dir, root_folder_id)
        with stage_boundary_lock():
            covering = local_root_for_folder(db, root_folder_id)
            if covering is not None:
                if covering == root_folder_id:
                    raise LocalWorkspaceError("This folder is already staged locally")
                raise LocalWorkspaceError(f"This folder is already covered by local folder {covering}")
            _remove_folder_dir(vireo_dir, root_folder_id)
            roots, folders = _catalog_records(db, root_folder_id, base / "files")
            db.conn.execute("BEGIN IMMEDIATE")
            try:
                db.conn.execute(
                    "INSERT INTO local_folders (root_folder_id, state, created_at) VALUES (?, 'staging', ?)",
                    (root_folder_id, time.time()),
                )
                for folder in folders:
                    db.conn.execute(
                        """INSERT INTO local_folder_mappings
                           (root_folder_id, folder_id, source_path, local_path,
                            original_status, is_root)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            root_folder_id,
                            folder["folder_id"],
                            folder["source_path"],
                            folder["local_path"],
                            folder["status"],
                            1 if folder["is_root"] else 0,
                        ),
                    )
                db.conn.commit()
            except BaseException:
                db.conn.rollback()
                raise

        copied = 0
        copied_bytes = 0
        try:
            entries_per_root, total_files, total_bytes = _collect_source_entries(roots, base)
            manifest = {
                "version": MANIFEST_VERSION,
                "root_folder_id": root_folder_id,
                "created_at": time.time(),
                "total_files": total_files,
                "total_bytes": total_bytes,
                "roots": [dict(roots[0])],
                "files": [],
            }
            root = roots[0]
            os.makedirs(root["local_path"], exist_ok=True)
            for rel, source, st in entries_per_root[0]:
                if cancel_check and cancel_check():
                    raise LocalWorkspaceCancelled("Local folder transfer cancelled")
                destination = os.path.join(root["local_path"], rel)
                record = _copy_entry(source, destination, st, root["source_path"], cancel_check)
                if record is None:
                    continue
                record.update({"root": 0, "path": rel})
                manifest["files"].append(record)
                copied += 1
                copied_bytes += record.get("size", 0)
                if progress:
                    progress(copied, total_files, copied_bytes, total_bytes, rel)

            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("Local folder transfer cancelled")
            if begin_commit and not begin_commit():
                raise LocalWorkspaceCancelled("Local folder transfer cancelled")
            _write_manifest(manifest_path(vireo_dir, root_folder_id), manifest)

            db.conn.execute("BEGIN IMMEDIATE")
            try:
                for folder in folders:
                    db.conn.execute(
                        "UPDATE folders SET path=? WHERE id=? AND path=?",
                        (folder["local_path"], folder["folder_id"], folder["source_path"]),
                    )
                    if db.conn.execute("SELECT changes()").fetchone()[0] != 1:
                        raise LocalWorkspaceError(
                            f"Catalog folder changed while staging: {folder['source_path']}"
                        )
                db.conn.execute(
                    "UPDATE local_folders SET state='active', activated_at=? WHERE root_folder_id=?",
                    (time.time(), root_folder_id),
                )
                db.conn.commit()
            except BaseException:
                db.conn.rollback()
                raise
            for workspace_id in affected_workspace_ids(db, root_folder_id):
                db.invalidate_new_images_cache_for_workspace(workspace_id)
            return {
                "ok": True,
                "root_folder_id": root_folder_id,
                "files": total_files,
                "bytes": total_bytes,
                "local_path": str(base / "files"),
            }
        except BaseException:
            current = folder_state(db, root_folder_id)
            if current and current.get("state") == "staging":
                _remove_folder_dir(vireo_dir, root_folder_id)
                _delete_state_rows(db, root_folder_id)
                db.conn.commit()
            raise


def folder_status(db, root_folder_id: int, vireo_dir: str) -> dict:
    """Return status for a root, resolving a shared covering session."""
    root_folder_id = int(root_folder_id)
    covering = local_root_for_folder(db, root_folder_id)
    if covering is None:
        row = db.conn.execute("SELECT path FROM folders WHERE id=?", (root_folder_id,)).fetchone()
        return {
            "state": "remote",
            "folder_id": root_folder_id,
            "root_folder_id": root_folder_id,
            "source_path": row["path"] if row else None,
            "workspace_ids": workspace_ids_for_folder_tree(db, root_folder_id),
        }
    root_folder_id = covering
    state_row = folder_state(db, root_folder_id)
    root = _root_mapping(db, root_folder_id)
    if state_row is None or root is None:
        raise LocalWorkspaceError("Local folder state is incomplete")
    result = {
        "state": state_row["state"],
        "folder_id": root_folder_id,
        "root_folder_id": root_folder_id,
        "source_path": root["source_path"],
        "local_path": root["local_path"],
        "created_at": state_row.get("created_at"),
        "workspace_ids": affected_workspace_ids(db, root_folder_id),
    }
    manifest = None
    manifest_error = None
    try:
        manifest = _load_manifest(vireo_dir, root_folder_id)
    except LocalWorkspaceError as exc:
        manifest_error = str(exc)
    if manifest:
        result["total_files"] = manifest.get("total_files", 0)
        result["total_bytes"] = manifest.get("total_bytes", 0)
    if result["state"] == "staging":
        return result
    if result["state"] == "syncing":
        result["state"] = "recovery"
        result["recovery_kind"] = "sync"
        result.update(_change_summary(manifest, manifest_error))
        return result
    if _managed_root_state(root["local_path"]) != "ok":
        result["state"] = "recovery"
        result["missing_local_paths"] = [root["local_path"]]
        return result
    result.update(_change_summary(manifest, manifest_error))
    try:
        source_st = os.lstat(root["source_path"])
        result["source_available"] = stat.S_ISDIR(source_st.st_mode) and not stat.S_ISLNK(source_st.st_mode)
    except OSError:
        result["source_available"] = False
    return result


def workspace_status(db, workspace_id: int, vireo_dir: str) -> dict:
    """Aggregate the active workspace's root-folder residency."""
    roots = [dict(row) for row in db.get_workspace_folder_roots(workspace_id)]
    items = []
    seen_sessions = set()
    for root in roots:
        status = folder_status(db, int(root["id"]), vireo_dir)
        status["requested_folder_id"] = int(root["id"])
        status["display_path"] = status.get("source_path") or root["path"]
        status["workspace_photo_count"] = int(root.get("workspace_photo_count") or 0)
        items.append(status)
        if status["state"] != "remote":
            seen_sessions.add(status["root_folder_id"])
    local_count = sum(1 for item in items if item["state"] != "remote")
    state = "remote" if local_count == 0 else "active" if local_count == len(items) else "mixed"
    return {
        "state": state,
        "workspace_id": int(workspace_id),
        "folder_count": len(items),
        "local_folder_count": local_count,
        "session_count": len(seen_sessions),
        "folders": items,
    }


def _restore_catalog(db, root_folder_id: int) -> None:
    mappings = _mappings(db, root_folder_id)
    mapped_ids = {item["folder_id"] for item in mappings}
    root = next((item for item in mappings if item["is_root"]), None)
    if root is None:
        raise LocalWorkspaceError("Local folder mapping is missing its root")
    db.conn.execute("BEGIN IMMEDIATE")
    try:
        for mapping in mappings:
            conflict = db.conn.execute(
                "SELECT id FROM folders WHERE path=? AND id != ?",
                (mapping["source_path"], mapping["folder_id"]),
            ).fetchone()
            if conflict and conflict["id"] not in mapped_ids:
                db._merge_into_existing(
                    conflict["id"], mapping["folder_id"], mapping["source_path"], commit=False
                )
        for mapping in mappings:
            db.conn.execute(
                "UPDATE folders SET path=? WHERE id=?",
                (f"__vireo_local_folder_restore__/{root_folder_id}/{mapping['folder_id']}", mapping["folder_id"]),
            )
        for mapping in mappings:
            db.conn.execute(
                "UPDATE folders SET path=?, status=? WHERE id=?",
                (mapping["source_path"], mapping["original_status"], mapping["folder_id"]),
            )

        relinked = list(mapped_ids)
        for row in db.conn.execute("SELECT id, path, status FROM folders").fetchall():
            if row["id"] in mapped_ids or not _is_within(row["path"], root["local_path"]):
                continue
            target = os.path.normpath(
                os.path.join(root["source_path"], _relative(row["path"], root["local_path"]))
            )
            existing = db.conn.execute(
                "SELECT id FROM folders WHERE path=? AND id != ?", (target, row["id"])
            ).fetchone()
            if existing:
                db._merge_into_existing(row["id"], existing["id"], target, commit=False)
            else:
                db.conn.execute("UPDATE folders SET path=? WHERE id=?", (target, row["id"]))
                relinked.append(row["id"])
        db._relink_parents_by_path(relinked)
        _delete_state_rows(db, root_folder_id)
        db.conn.commit()
    except BaseException:
        db.conn.rollback()
        raise


def sync_folder(
    db,
    root_folder_id: int,
    vireo_dir: str,
    *,
    allow_deletions: bool = False,
    confirmed_deletions: int | None = None,
    progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Publish one shared local folder and restore its catalog paths."""
    root_folder_id = int(local_root_for_folder(db, root_folder_id) or root_folder_id)
    with _folder_lock(root_folder_id):
        state_row = folder_state(db, root_folder_id)
        if not state_row or state_row["state"] not in {"active", "syncing"}:
            raise LocalWorkspaceError("This folder is not working locally")
        resuming = state_row["state"] == "syncing"
        recovery_confirmed = None
        if resuming:
            allow_deletions = True
            recovery_confirmed = _load_sync_recovery(vireo_dir, root_folder_id)
        manifest = _load_manifest(vireo_dir, root_folder_id)
        if manifest is None:
            raise LocalWorkspaceError(
                "The staged file inventory is missing. Discard restores the catalog without touching source files."
            )
        root = manifest["roots"][0]
        try:
            source_st = os.lstat(root["source_path"])
        except OSError:
            raise LocalWorkspaceError(f"Source storage is unavailable: {root['source_path']}") from None
        if stat.S_ISLNK(source_st.st_mode) or not stat.S_ISDIR(source_st.st_mode):
            raise LocalWorkspaceError(f"Source storage is unavailable or unsafe: {root['source_path']}")
        if _managed_root_state(root["local_path"]) != "ok":
            raise LocalWorkspaceError(
                f"Managed local folder is unavailable: {root['local_path']}. Restore it or discard the local copy."
            )

        baseline, local, changed, deleted = _local_changes(manifest)
        recovery_republish = set()
        if resuming and recovery_confirmed:
            changed_set = set(changed)
            for key in recovery_confirmed:
                if key in local:
                    recovery_republish.add(key)
                    if key not in changed_set:
                        changed.append(key)
                        changed_set.add(key)
        if deleted and not allow_deletions:
            raise LocalWorkspaceError(f"Local work deleted {len(deleted)} file(s); confirm deletions before syncing")
        if confirmed_deletions is not None and len(deleted) > confirmed_deletions:
            raise LocalWorkspaceError(
                f"Local deletions changed since you confirmed: {len(deleted)} file(s) would now be deleted."
            )
        fresh_confirmation = resuming and confirmed_deletions == len(deleted)
        if resuming and recovery_confirmed is not None and not fresh_confirmation:
            new_deletions = [key for key in deleted if key not in recovery_confirmed]
            if new_deletions:
                raise LocalWorkspaceError(
                    "Local deletions changed since sync was interrupted; review and confirm again."
                )

        conflicts = []
        at_risk = [key for key in changed if key in baseline] + list(deleted)
        for key in at_risk:
            index, rel = key
            remote_path = os.path.join(manifest["roots"][index]["source_path"], rel)
            remote_matches, remote_sha = _source_state(remote_path, baseline[key], cancel_check)
            if remote_matches:
                continue
            entry = local.get(key)
            local_path = entry[0] if entry else None
            if local_path is None and not os.path.lexists(remote_path):
                continue
            if key in recovery_republish and not os.path.lexists(remote_path):
                continue
            if local_path and _matches_remote(local_path, remote_path, remote_sha, cancel_check):
                continue
            conflicts.append(remote_path)

        deleted_set = set(deleted)
        for key in changed:
            if key in baseline:
                continue
            index, rel = key
            remote_path = os.path.join(manifest["roots"][index]["source_path"], rel)
            if not os.path.lexists(remote_path):
                continue
            if os.path.isdir(remote_path) and not os.path.islink(remote_path):
                conflicts.extend(
                    full
                    for entry_rel, full, st in _walk_entries(remote_path)
                    if _entry_type(st) != "dir" and (index, os.path.join(rel, entry_rel)) not in deleted_set
                )
            elif not _matches_remote(local[key][0], remote_path, None, cancel_check):
                conflicts.append(remote_path)
        conflicts.extend(_ancestor_conflicts(list(changed) + list(deleted), manifest, deleted_set))
        if conflicts:
            raise LocalWorkspaceConflict(sorted(set(conflicts)))

        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Local folder sync cancelled")
        if begin_commit and not begin_commit():
            raise LocalWorkspaceCancelled("Local folder sync cancelled")
        if not resuming:
            _write_sync_recovery(vireo_dir, root_folder_id, deleted)
            db.conn.execute(
                "UPDATE local_folders SET state='syncing' WHERE root_folder_id=?", (root_folder_id,)
            )
            db.conn.commit()
        elif fresh_confirmation:
            _write_sync_recovery(vireo_dir, root_folder_id, deleted)

        total = len(changed) + len(deleted)
        done = 0
        for index, rel in deleted:
            remote_path = os.path.join(manifest["roots"][index]["source_path"], rel)
            with suppress(FileNotFoundError):
                os.unlink(remote_path)
            done += 1
            if progress:
                progress(done, total, rel)
        for key in changed:
            index, rel = key
            remote_path = os.path.join(manifest["roots"][index]["source_path"], rel)
            _prepare_publish_target(remote_path)
            _atomic_publish(local[key][0], remote_path)
            done += 1
            if progress:
                progress(done, total, rel)
        workspace_ids = affected_workspace_ids(db, root_folder_id)
        _restore_catalog(db, root_folder_id)
        _remove_folder_dir(vireo_dir, root_folder_id)
        for workspace_id in workspace_ids:
            db.invalidate_new_images_cache_for_workspace(workspace_id)
        return {
            "ok": True,
            "root_folder_id": root_folder_id,
            "created_or_modified": len(changed),
            "deleted": len(deleted),
            "files_examined": len(local),
        }


def discard_folder(db, root_folder_id: int, vireo_dir: str, *, acknowledge_published=False) -> dict:
    """Remove one local session without changing source files."""
    root_folder_id = int(local_root_for_folder(db, root_folder_id) or root_folder_id)
    with _folder_lock(root_folder_id):
        state_row = folder_state(db, root_folder_id)
        if not state_row:
            raise LocalWorkspaceError("This folder is not working locally")
        state = state_row["state"]
        if state == "staging":
            _remove_folder_dir(vireo_dir, root_folder_id)
            _delete_state_rows(db, root_folder_id)
            db.conn.commit()
            return {"ok": True, "root_folder_id": root_folder_id, "discarded": True}
        if state == "syncing" and not acknowledge_published:
            raise LocalWorkspaceError(
                "A sync-back was interrupted after some files were published. Finish syncing, or acknowledge that unpublished changes will be lost."
            )
        if state not in {"active", "syncing"}:
            raise LocalWorkspaceError("Local folder is not in a recoverable state")
        workspace_ids = affected_workspace_ids(db, root_folder_id)
        _restore_catalog(db, root_folder_id)
        _remove_folder_dir(vireo_dir, root_folder_id)
        for workspace_id in workspace_ids:
            db.invalidate_new_images_cache_for_workspace(workspace_id)
        return {"ok": True, "root_folder_id": root_folder_id, "discarded": True}
