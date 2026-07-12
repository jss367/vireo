"""Managed local copies for workspaces backed by slower storage.

The catalog continues to be the path source of truth.  Once staging has
finished successfully, folder paths belonging exclusively to the workspace are
rebased to a managed local tree.  Sync-back publishes only local changes, then
restores the original catalog paths and removes the managed copy.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path

MANIFEST_VERSION = 1
LOCAL_RESERVE_BYTES = 1024**3
_LOCK = threading.RLock()


class LocalWorkspaceError(RuntimeError):
    """A user-actionable local workspace failure."""


class LocalWorkspaceConflict(LocalWorkspaceError):
    """The source changed after staging and must not be overwritten."""

    def __init__(self, paths: list[str]):
        self.paths = paths
        preview = ", ".join(paths[:3])
        if len(paths) > 3:
            preview += f", and {len(paths) - 3} more"
        super().__init__(f"Files changed on the source while working locally: {preview}")


class LocalWorkspaceCancelled(LocalWorkspaceError):
    """The caller cancelled a still-interruptible transfer."""


def workspace_dir(vireo_dir: str, workspace_id: int) -> Path:
    return Path(vireo_dir) / "local-workspaces" / str(int(workspace_id))


def manifest_path(vireo_dir: str, workspace_id: int) -> Path:
    return workspace_dir(vireo_dir, workspace_id) / "manifest.json"


def has_local_workspace(vireo_dir: str, workspace_id: int) -> bool:
    return manifest_path(vireo_dir, workspace_id).is_file()


def _write_manifest(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _load_manifest(vireo_dir: str, workspace_id: int) -> dict | None:
    path = manifest_path(vireo_dir, workspace_id)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LocalWorkspaceError(f"Local workspace manifest is unreadable: {exc}") from exc
    if data.get("version") != MANIFEST_VERSION:
        raise LocalWorkspaceError("Local workspace manifest was created by an unsupported Vireo version")
    if int(data.get("workspace_id", -1)) != int(workspace_id):
        raise LocalWorkspaceError("Local workspace manifest belongs to another workspace")
    return data


def _norm(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def _is_within(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([_norm(path), _norm(root)]) == _norm(root)
    except ValueError:
        return False


def _physical_is_within(path: str, root: str) -> bool:
    try:
        resolved_path = os.path.normcase(os.path.realpath(path))
        resolved_root = os.path.normcase(os.path.realpath(root))
        return os.path.commonpath([resolved_path, resolved_root]) == resolved_root
    except ValueError:
        return False


def _relative(path: str, root: str) -> str:
    rel = os.path.relpath(path, root)
    if rel == os.curdir:
        return ""
    if rel == os.pardir or rel.startswith(os.pardir + os.sep):
        raise LocalWorkspaceError(f"Path is outside workspace root: {path}")
    return rel


def _sha256(path: str, cancel_check=None) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _raise_walk_error(error: OSError) -> None:
    raise LocalWorkspaceError(f"Could not read workspace directory: {error}") from error


def _walk_entries(root: str):
    """Yield relative paths for regular files and symlinks without following links."""
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False, onerror=_raise_walk_error):
        for dirname in list(dirnames):
            full = os.path.join(dirpath, dirname)
            if os.path.islink(full):
                dirnames.remove(dirname)
                yield _relative(full, root), full
        for filename in filenames:
            full = os.path.join(dirpath, filename)
            yield _relative(full, root), full


def _preflight_sources(roots: list[dict], local_base: Path) -> tuple[int, int]:
    total_bytes = 0
    total_files = 0
    for root in roots:
        source = root["source_path"]
        if not os.path.isdir(source):
            raise LocalWorkspaceError(f"Workspace folder is unavailable: {source}")
        for _rel, full in _walk_entries(source):
            mode = os.lstat(full).st_mode
            if stat.S_ISREG(mode):
                total_bytes += os.lstat(full).st_size
            elif not stat.S_ISLNK(mode):
                raise LocalWorkspaceError(f"Unsupported special file in workspace: {full}")
            total_files += 1

    local_base.parent.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(local_base.parent).free
    required = total_bytes + LOCAL_RESERVE_BYTES
    if free < required:
        raise LocalWorkspaceError(
            f"Not enough local space: need {required:,} bytes including safety reserve, "
            f"but only {free:,} bytes are free"
        )
    return total_files, total_bytes


def _copy_regular_with_hash(source: str, destination: str, cancel_check=None) -> dict:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    for attempt in range(2):
        before = os.stat(source, follow_symlinks=False)
        tmp = destination + ".vireo-copying"
        digest = hashlib.sha256()
        try:
            with open(source, "rb") as src, open(tmp, "wb") as dst:
                while True:
                    if cancel_check and cancel_check():
                        raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                    digest.update(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            shutil.copystat(source, tmp, follow_symlinks=False)
            after = os.stat(source, follow_symlinks=False)
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                os.unlink(tmp)
                if attempt == 0:
                    continue
                raise LocalWorkspaceError(f"Source kept changing while it was copied: {source}")
            os.replace(tmp, destination)
            return {
                "type": "file",
                "size": after.st_size,
                "mtime_ns": after.st_mtime_ns,
                "sha256": digest.hexdigest(),
            }
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(tmp)
            raise
    raise AssertionError("copy retry loop exhausted")


def _copy_entry(source: str, destination: str, cancel_check=None) -> dict:
    mode = os.lstat(source).st_mode
    if stat.S_ISLNK(mode):
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        target = os.readlink(source)
        with suppress(FileNotFoundError):
            os.unlink(destination)
        os.symlink(target, destination)
        return {"type": "symlink", "target": target}
    if not stat.S_ISREG(mode):
        raise LocalWorkspaceError(f"Unsupported special file in workspace: {source}")
    return _copy_regular_with_hash(source, destination, cancel_check)


def _copy_directory_structure(source_root: str, local_root: str) -> None:
    """Create real source directories locally, including empty directories."""
    for dirpath, dirnames, _filenames in os.walk(source_root, followlinks=False, onerror=_raise_walk_error):
        rel_dir = _relative(dirpath, source_root)
        local_dir = os.path.join(local_root, rel_dir)
        os.makedirs(local_dir, exist_ok=True)
        # Directory symlinks are manifest entries, not real directories.
        dirnames[:] = [name for name in dirnames if not os.path.islink(os.path.join(dirpath, name))]


def _root_records(db, workspace_id: int, local_base: Path) -> tuple[list[dict], list[dict]]:
    roots = [dict(row) for row in db.get_workspace_folder_roots(workspace_id)]
    folders = [dict(row) for row in db.get_workspace_folders(workspace_id)]
    if not roots:
        raise LocalWorkspaceError("Add at least one folder before working locally")

    normalized = sorted((_norm(row["path"]), row) for row in roots)
    for index, (path, _row) in enumerate(normalized):
        for other, _other_row in normalized[index + 1 :]:
            if _is_within(other, path):
                raise LocalWorkspaceError("Workspace roots overlap; remove the nested root before working locally")

    folder_ids = [row["id"] for row in folders]
    placeholders = ",".join("?" for _ in folder_ids)
    shared = db.conn.execute(
        f"SELECT f.path FROM workspace_folders wf JOIN folders f ON f.id=wf.folder_id "
        f"WHERE wf.folder_id IN ({placeholders}) AND wf.workspace_id != ? LIMIT 1",
        [*folder_ids, workspace_id],
    ).fetchone()
    if shared:
        raise LocalWorkspaceError(
            f"Folder is also used by another workspace: {shared['path']}. "
            "Remove the shared folder there before working locally."
        )

    # A recursive root in another workspace may cover one of these folders
    # without having materialized the exact workspace_folders row yet. Since
    # folders.path is global, rebasing that covered folder would still break
    # the other workspace. Check path coverage in both directions as well as
    # the exact-ID links above.
    other_roots = db.conn.execute(
        """SELECT f.path
           FROM workspace_folders wf
           JOIN folders f ON f.id = wf.folder_id
           WHERE wf.workspace_id != ? AND wf.is_root = 1""",
        (workspace_id,),
    ).fetchall()
    for folder in folders:
        for other_root in other_roots:
            if _is_within(folder["path"], other_root["path"]) or _is_within(other_root["path"], folder["path"]):
                raise LocalWorkspaceError(
                    f"Folder overlaps a root used by another workspace: {other_root['path']}. "
                    "Remove the overlapping folder there before working locally."
                )

    root_records = []
    for index, row in enumerate(roots):
        name = Path(row["path"].rstrip("/\\")).name or f"root-{row['id']}"
        safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
        root_records.append(
            {
                "folder_id": row["id"],
                "source_path": row["path"],
                "local_path": str(local_base / f"{index + 1}-{safe_name}"),
            }
        )

    for root in root_records:
        if _physical_is_within(root["local_path"], root["source_path"]) or _physical_is_within(
            root["source_path"], root["local_path"]
        ):
            raise LocalWorkspaceError(
                "Managed local storage overlaps a workspace source folder; "
                "move Vireo's data directory to local storage first"
            )

    folder_records = []
    for folder in folders:
        matches = [root for root in root_records if _is_within(folder["path"], root["source_path"])]
        if not matches:
            raise LocalWorkspaceError(f"Workspace folder is not beneath a workspace root: {folder['path']}")
        root = max(matches, key=lambda item: len(_norm(item["source_path"])))
        local_path = os.path.join(root["local_path"], _relative(folder["path"], root["source_path"]))
        folder_records.append(
            {
                "folder_id": folder["id"],
                "source_path": folder["path"],
                "local_path": os.path.normpath(local_path),
            }
        )
    return root_records, folder_records


def stage_workspace(db, workspace_id: int, vireo_dir: str, *, progress=None, cancel_check=None) -> dict:
    """Copy a workspace locally and atomically rebase its catalog paths."""
    with _LOCK:
        if _load_manifest(vireo_dir, workspace_id):
            raise LocalWorkspaceError("This workspace is already staged locally")

        base = workspace_dir(vireo_dir, workspace_id)
        roots, folders = _root_records(db, workspace_id, base / "files")
        total_files, total_bytes = _preflight_sources(roots, base)
        manifest = {
            "version": MANIFEST_VERSION,
            "workspace_id": workspace_id,
            "state": "staging",
            "created_at": time.time(),
            "total_files": total_files,
            "total_bytes": total_bytes,
            "roots": roots,
            "folders": folders,
            "files": [],
        }
        _write_manifest(manifest_path(vireo_dir, workspace_id), manifest)

        copied = 0
        copied_bytes = 0
        try:
            for root_index, root in enumerate(roots):
                os.makedirs(root["local_path"], exist_ok=True)
                _copy_directory_structure(root["source_path"], root["local_path"])
                for rel, source in _walk_entries(root["source_path"]):
                    if cancel_check and cancel_check():
                        raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
                    destination = os.path.join(root["local_path"], rel)
                    record = _copy_entry(source, destination, cancel_check)
                    record.update({"root": root_index, "path": rel})
                    manifest["files"].append(record)
                    copied += 1
                    copied_bytes += record.get("size", 0)
                    if progress:
                        progress(copied, total_files, copied_bytes, total_bytes, rel)

            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("Local workspace transfer cancelled")

            # Persist the recovery boundary before changing catalog paths. If
            # Vireo exits from this point onward, Discard can safely detect
            # and reverse whichever path updates committed.
            manifest["state"] = "activating"
            _write_manifest(manifest_path(vireo_dir, workspace_id), manifest)
            db.conn.execute("BEGIN IMMEDIATE")
            try:
                for folder in folders:
                    db.conn.execute(
                        "UPDATE folders SET path=?, status='ok' WHERE id=? AND path=?",
                        (folder["local_path"], folder["folder_id"], folder["source_path"]),
                    )
                    if db.conn.execute("SELECT changes()").fetchone()[0] != 1:
                        raise LocalWorkspaceError(f"Catalog folder changed while staging: {folder['source_path']}")
                db.conn.commit()
            except BaseException:
                db.conn.rollback()
                raise

            manifest["state"] = "active"
            manifest["activated_at"] = time.time()
            _write_manifest(manifest_path(vireo_dir, workspace_id), manifest)
            return {
                "ok": True,
                "files": total_files,
                "bytes": total_bytes,
                "local_path": str(base / "files"),
            }
        except BaseException:
            # A staging manifest is safe to remove because catalog paths have
            # not yet changed. Activating/active manifests are recovery data.
            current = _load_manifest(vireo_dir, workspace_id)
            if current and current.get("state") == "staging":
                shutil.rmtree(base, ignore_errors=True)
            raise


def _entry_state(path: str, *, hash_file=False, cancel_check=None) -> dict | None:
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(mode):
        return {"type": "symlink", "target": os.readlink(path)}
    if not stat.S_ISREG(mode):
        return {"type": "special"}
    info = os.stat(path, follow_symlinks=False)
    result = {"type": "file", "size": info.st_size, "mtime_ns": info.st_mtime_ns}
    if hash_file:
        result["sha256"] = _sha256(path, cancel_check)
    return result


def _same_as_baseline(path: str, baseline: dict, cancel_check=None, *, force_hash: bool = False) -> bool:
    current = _entry_state(path)
    if current is None or current.get("type") != baseline.get("type"):
        return False
    if current["type"] == "symlink":
        return current.get("target") == baseline.get("target")
    if current.get("size") != baseline.get("size"):
        return False
    if not force_hash and current.get("mtime_ns") == baseline.get("mtime_ns"):
        return True
    return _sha256(path, cancel_check) == baseline.get("sha256")


def _same_metadata(path: str, baseline: dict) -> bool:
    """Cheap status-page comparison; sync-back performs content hashing."""
    current = _entry_state(path)
    if current is None or current.get("type") != baseline.get("type"):
        return False
    if current["type"] == "symlink":
        return current.get("target") == baseline.get("target")
    return current.get("size") == baseline.get("size") and current.get("mtime_ns") == baseline.get("mtime_ns")


def _same_content(first: str, second: str, cancel_check=None) -> bool:
    first_state = _entry_state(first)
    second_state = _entry_state(second)
    if first_state is None or second_state is None:
        return first_state is None and second_state is None
    if first_state.get("type") != second_state.get("type"):
        return False
    if first_state["type"] == "symlink":
        return first_state.get("target") == second_state.get("target")
    if first_state["type"] != "file" or first_state.get("size") != second_state.get("size"):
        return False
    return _sha256(first, cancel_check) == _sha256(second, cancel_check)


def _manifest_maps(manifest: dict) -> tuple[dict, dict]:
    baseline = {(item["root"], item["path"]): item for item in manifest["files"]}
    local = {}
    for root_index, root in enumerate(manifest["roots"]):
        if not os.path.isdir(root["local_path"]):
            continue
        for rel, full in _walk_entries(root["local_path"]):
            local[(root_index, rel)] = full
    return baseline, local


def status(db, workspace_id: int, vireo_dir: str) -> dict:
    """Return lightweight local-workspace state and local change counts."""
    manifest = _load_manifest(vireo_dir, workspace_id)
    if not manifest:
        return {"state": "remote", "workspace_id": workspace_id}
    result = {
        "state": manifest.get("state", "unknown"),
        "workspace_id": workspace_id,
        "created_at": manifest.get("created_at"),
        "local_path": str(workspace_dir(vireo_dir, workspace_id) / "files"),
        "source_paths": [root["source_path"] for root in manifest.get("roots", [])],
        "total_files": manifest.get("total_files", 0),
        "total_bytes": manifest.get("total_bytes", 0),
    }
    if result["state"] == "activating":
        result["state"] = "recovery"
        return result
    if result["state"] != "active":
        return result

    baseline, local = _manifest_maps(manifest)
    created = 0
    modified = 0
    deleted = 0
    for key, item in baseline.items():
        path = local.get(key)
        if path is None:
            deleted += 1
        elif not _same_metadata(path, item):
            modified += 1
    created = len(set(local) - set(baseline))
    result["changes"] = {"created": created, "modified": modified, "deleted": deleted}
    result["source_available"] = all(os.path.isdir(root["source_path"]) for root in manifest["roots"])
    return result


def _preflight_restore_paths(db, manifest: dict) -> list[tuple[int, str, str]]:
    """Map all catalog folders under local roots back to source locations."""
    mappings = []
    rows = db.conn.execute("SELECT id, path FROM folders").fetchall()
    own_ids = set()
    for row in rows:
        matches = [root for root in manifest["roots"] if _is_within(row["path"], root["local_path"])]
        if not matches:
            continue
        root = max(matches, key=lambda item: len(_norm(item["local_path"])))
        target = os.path.normpath(os.path.join(root["source_path"], _relative(row["path"], root["local_path"])))
        mappings.append((row["id"], row["path"], target))
        own_ids.add(row["id"])

    for folder_id, _old, target in mappings:
        conflict = db.conn.execute("SELECT id FROM folders WHERE path=? AND id != ?", (target, folder_id)).fetchone()
        if conflict and conflict["id"] not in own_ids:
            raise LocalWorkspaceError(f"Cannot restore catalog path because it is already tracked: {target}")
    return mappings


def _atomic_publish(local_path: str, remote_path: str) -> None:
    os.makedirs(os.path.dirname(remote_path), exist_ok=True)
    fd, temp = tempfile.mkstemp(
        prefix=f".{os.path.basename(remote_path)}.vireo-syncing-",
        dir=os.path.dirname(remote_path),
    )
    os.close(fd)
    if os.path.islink(local_path):
        try:
            os.unlink(temp)
            os.symlink(os.readlink(local_path), temp)
            os.replace(temp, remote_path)
            return
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(temp)
            raise
    try:
        shutil.copy2(local_path, temp, follow_symlinks=False)
        with open(temp, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temp, remote_path)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temp)
        raise


def sync_back(
    db,
    workspace_id: int,
    vireo_dir: str,
    *,
    allow_deletions: bool = False,
    progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Publish local changes, restore source paths, and remove the local copy."""
    with _LOCK:
        manifest = _load_manifest(vireo_dir, workspace_id)
        if not manifest or manifest.get("state") != "active":
            raise LocalWorkspaceError("This workspace is not working locally")
        for root in manifest["roots"]:
            if not os.path.isdir(root["source_path"]):
                raise LocalWorkspaceError(f"Source storage is unavailable: {root['source_path']}")

        restore_mappings = _preflight_restore_paths(db, manifest)
        baseline, local = _manifest_maps(manifest)
        deleted = sorted(set(baseline) - set(local))
        if deleted and not allow_deletions:
            raise LocalWorkspaceError(f"Local work deleted {len(deleted)} file(s); confirm deletions before syncing")

        conflicts = []
        for key, original in baseline.items():
            root_index, rel = key
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            if _same_as_baseline(remote_path, original, cancel_check, force_hash=True):
                continue
            local_path = local.get(key)
            # A prior interrupted sync may already have published this exact
            # result. Treat it as resumable, not as an outside conflict.
            if local_path is None and not os.path.lexists(remote_path):
                continue
            if local_path and os.path.lexists(remote_path) and _same_content(local_path, remote_path, cancel_check):
                continue
            conflicts.append(remote_path)

        for key in set(local) - set(baseline):
            root_index, rel = key
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            if os.path.lexists(remote_path) and not _same_content(local[key], remote_path, cancel_check):
                conflicts.append(remote_path)

        if conflicts:
            raise LocalWorkspaceConflict(sorted(set(conflicts)))

        changed = []
        for key, local_path in local.items():
            original = baseline.get(key)
            if original is None or not _same_as_baseline(local_path, original, force_hash=True):
                changed.append((key, local_path))

        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Local workspace sync cancelled")
        if begin_commit and not begin_commit():
            raise LocalWorkspaceCancelled("Local workspace sync cancelled")

        total = len(changed) + len(deleted)
        done = 0
        for (root_index, rel), local_path in changed:
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            _atomic_publish(local_path, remote_path)
            done += 1
            if progress:
                progress(done, total, rel)
        for root_index, rel in deleted:
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            with suppress(FileNotFoundError):
                os.unlink(remote_path)
            done += 1
            if progress:
                progress(done, total, rel)

        # Use temporary unique paths so swaps/nesting can never trip the
        # folders.path UNIQUE constraint midway through restoration.
        db.conn.execute("BEGIN IMMEDIATE")
        try:
            for folder_id, _old, _target in restore_mappings:
                db.conn.execute(
                    "UPDATE folders SET path=? WHERE id=?",
                    (f"__vireo_local_restore__/{workspace_id}/{folder_id}", folder_id),
                )
            for folder_id, _old, target in restore_mappings:
                db.conn.execute("UPDATE folders SET path=?, status='ok' WHERE id=?", (target, folder_id))
            db._relink_parents_by_path([item[0] for item in restore_mappings])
            db.conn.commit()
        except BaseException:
            db.conn.rollback()
            raise

        shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
        return {
            "ok": True,
            "created_or_modified": len(changed),
            "deleted": len(deleted),
            "files_examined": len(local),
        }


def discard_local(db, workspace_id: int, vireo_dir: str) -> dict:
    """Restore catalog paths and remove local changes without touching source files."""
    with _LOCK:
        manifest = _load_manifest(vireo_dir, workspace_id)
        if not manifest:
            raise LocalWorkspaceError("This workspace is not working locally")
        if manifest.get("state") == "staging":
            shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
            return {"ok": True, "discarded": True}
        if manifest.get("state") not in {"active", "activating"}:
            raise LocalWorkspaceError("Local workspace is not in a recoverable state")

        mappings = _preflight_restore_paths(db, manifest)
        db.conn.execute("BEGIN IMMEDIATE")
        try:
            for folder_id, _old, _target in mappings:
                db.conn.execute(
                    "UPDATE folders SET path=? WHERE id=?",
                    (f"__vireo_local_discard__/{workspace_id}/{folder_id}", folder_id),
                )
            for folder_id, _old, target in mappings:
                db.conn.execute("UPDATE folders SET path=?, status='ok' WHERE id=?", (target, folder_id))
            db._relink_parents_by_path([item[0] for item in mappings])
            db.conn.commit()
        except BaseException:
            db.conn.rollback()
            raise
        shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
        return {"ok": True, "discarded": True}
