"""Managed local copies for workspaces backed by slower storage.

The catalog continues to be the path source of truth.  Once staging has
finished successfully, folder paths belonging exclusively to the workspace are
rebased to a managed local tree.  Sync-back publishes only local changes, then
restores the original catalog paths and removes the managed copy.

Lifecycle state and the folder path mapping live in SQLite
(``local_workspaces`` / ``local_workspace_folders``) so every state
transition commits in the same transaction as the catalog path updates it
describes — there is no window where the catalog and the recorded state can
disagree.  The JSON manifest beside the copied files is a pure file
inventory (the staged baseline used for change detection and conflict
checks), never a state store.

States: ``staging`` (copy in progress or interrupted; catalog untouched),
``active`` (catalog rebased to the local tree), ``syncing`` (a sync-back
began publishing to the source and did not finish).
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

from config import _replace_with_windows_retry

MANIFEST_VERSION = 2
LOCAL_RESERVE_BYTES = 1024**3

# Serializes stage/sync/discard per workspace. A short global guard covers
# stage's cross-workspace overlap validation so two concurrent stages cannot
# both pass the guards before either records its claim.
_LOCKS: dict[int, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()
_STAGE_GUARD = threading.Lock()


def _workspace_lock(workspace_id: int) -> threading.RLock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(int(workspace_id), threading.RLock())


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


def _sync_recovery_path(vireo_dir: str, workspace_id: int) -> Path:
    """The confirmed-deletion set persisted at the sync recovery boundary."""
    return workspace_dir(vireo_dir, workspace_id) / "sync-recovery.json"


def _write_sync_recovery(vireo_dir: str, workspace_id: int, deleted_keys) -> None:
    """Record the deletion set the user confirmed before the sync began.

    A resumed sync must not silently authorize any additional source
    deletion that appeared after the first attempt was interrupted — the
    user only confirmed the deletions listed here.
    """
    path = _sync_recovery_path(vireo_dir, workspace_id)
    payload = {
        "confirmed_deletions": [[int(root_index), rel] for root_index, rel in deleted_keys],
    }
    _write_manifest(path, payload)


def _load_sync_recovery(vireo_dir: str, workspace_id: int) -> set | None:
    """Return the confirmed-deletion key set from the sync recovery file, or None."""
    path = _sync_recovery_path(vireo_dir, workspace_id)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        raise LocalWorkspaceError(f"Sync recovery marker is unreadable: {exc}") from exc
    confirmed = data.get("confirmed_deletions")
    if not isinstance(confirmed, list):
        return set()
    result = set()
    for entry in confirmed:
        if isinstance(entry, list) and len(entry) == 2:
            root_index, rel = entry
            try:
                result.add((int(root_index), str(rel)))
            except (TypeError, ValueError):
                continue
    return result


def local_state(db, workspace_id: int) -> dict | None:
    """Return the workspace's local-workspace state row, or None."""
    row = db.conn.execute(
        "SELECT workspace_id, state, created_at, activated_at FROM local_workspaces WHERE workspace_id=?",
        (workspace_id,),
    ).fetchone()
    return dict(row) if row else None


def has_local_workspace(db, workspace_id: int) -> bool:
    return local_state(db, workspace_id) is not None


def folder_has_local_workspace(db, folder_id: int) -> tuple[bool, int | None]:
    """True if this folder is covered by any workspace's local_workspace_folders row.

    Folder-level mutations (relocate/delete on ``/api/folders/<id>``) rebase or
    remove the ``folders`` row that ``local_workspace_folders`` and the manifest
    point at, so a later sync/discard would be unable to restore the catalog.
    Returns the workspace_id so callers can name it in the refusal message.
    """
    row = db.conn.execute(
        """SELECT lwf.workspace_id
           FROM local_workspace_folders lwf
           JOIN local_workspaces lw ON lw.workspace_id = lwf.workspace_id
           WHERE lwf.folder_id = ?
           LIMIT 1""",
        (folder_id,),
    ).fetchone()
    if row is None:
        return False, None
    return True, int(row["workspace_id"])


def _db_mappings(db, workspace_id: int) -> list[dict]:
    rows = db.conn.execute(
        """SELECT folder_id, source_path, local_path, original_status, is_root, root_index
           FROM local_workspace_folders WHERE workspace_id=?
           ORDER BY is_root DESC, root_index, folder_id""",
        (workspace_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _db_roots(db, workspace_id: int) -> list[dict]:
    return [m for m in _db_mappings(db, workspace_id) if m["is_root"]]


def _write_manifest(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    # Windows AV/indexers can hold the destination open transiently; use the
    # same retry-aware replace as config writes.
    _replace_with_windows_retry(str(tmp), str(path))


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
    except (ValueError, OSError):
        return False


def _symlink_stays_within(path: str, root: str) -> bool:
    target = os.readlink(path)
    if os.path.isabs(target):
        return False
    parent_rel = os.path.relpath(os.path.dirname(path), root)
    lexical_parts = [] if parent_rel == os.curdir else list(Path(parent_rel).parts)
    for part in Path(target).parts:
        if part in {"", os.curdir}:
            continue
        if part == os.pardir:
            if not lexical_parts:
                return False
            lexical_parts.pop()
        else:
            lexical_parts.append(part)
    resolved = os.path.join(os.path.dirname(path), target)
    return _physical_is_within(resolved, root)


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


def _dest_case_insensitive(local_base: Path) -> bool:
    """Probe whether the local base directory's filesystem folds case.

    Two source paths that only differ in case (``Bird.jpg`` vs
    ``bird.jpg``) copy to the same destination on Windows/macOS defaults;
    the second silently overwrites the first while the manifest still
    records both entries. Callers use this probe to reject such source
    pairs before any file is copied.
    """
    base = local_base.parent
    base.mkdir(parents=True, exist_ok=True)
    fd, probe = tempfile.mkstemp(prefix="VireoCaseProbe-", dir=str(base))
    os.close(fd)
    try:
        lower = os.path.join(os.path.dirname(probe), os.path.basename(probe).lower())
        return lower != probe and os.path.exists(lower)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(probe)


def _walk_entries(root: str):
    """Yield ``(rel, full, lstat)`` for every entry under root.

    Directories are included (so callers can recreate structure, including
    empty directories); directory symlinks are yielded as link entries and
    not descended into. Each entry is stat'ed exactly once here — callers
    classify via the yielded stat instead of re-statting.
    """
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False, onerror=_raise_walk_error):
        rel_dir = _relative(dirpath, root)
        if rel_dir:
            yield rel_dir, dirpath, os.lstat(dirpath)
        kept = []
        for dirname in dirnames:
            full = os.path.join(dirpath, dirname)
            st = os.lstat(full)
            if stat.S_ISLNK(st.st_mode):
                yield _relative(full, root), full, st
            else:
                kept.append(dirname)
        dirnames[:] = kept
        for filename in filenames:
            full = os.path.join(dirpath, filename)
            yield rel_dir and os.path.join(rel_dir, filename) or filename, full, os.lstat(full)


def _collect_source_entries(roots: list[dict], local_base: Path) -> tuple[dict[int, list], int, int]:
    """Walk every source root once; validate entries and total up the copy.

    Returns (entries per root index, total_files, total_bytes). The entry
    lists are reused by the copy loop so staging enumerates the (slow)
    source storage exactly once.
    """
    per_root: dict[int, list] = {}
    total_bytes = 0
    total_files = 0
    local_base.parent.mkdir(parents=True, exist_ok=True)
    case_insensitive_target = _dest_case_insensitive(local_base)
    for index, root in enumerate(roots):
        source = root["source_path"]
        # os.path.isdir follows symlinks; a source root that is itself a
        # symlink would activate successfully here but sync_back later lstats
        # and rejects it, leaving every local edit unsyncable. Reject
        # symlinked (or non-directory) roots up front instead.
        try:
            root_st = os.lstat(source)
        except OSError as exc:
            raise LocalWorkspaceError(f"Workspace folder is unavailable: {source}") from exc
        if stat.S_ISLNK(root_st.st_mode):
            raise LocalWorkspaceError(
                f"Workspace root is a symlink and cannot be staged: {source}"
            )
        if not stat.S_ISDIR(root_st.st_mode):
            raise LocalWorkspaceError(f"Workspace folder is unavailable: {source}")
        entries = []
        folded_seen: dict[str, str] = {}
        for rel, full, st in _walk_entries(source):
            mode = st.st_mode
            if stat.S_ISREG(mode):
                total_bytes += st.st_size
                total_files += 1
            elif stat.S_ISLNK(mode):
                if not _symlink_stays_within(full, source):
                    raise LocalWorkspaceError(
                        f"Symlink escapes or uses an absolute target and cannot be staged: {full}"
                    )
                total_files += 1
            elif stat.S_ISDIR(mode):
                pass
            else:
                raise LocalWorkspaceError(f"Unsupported special file in workspace: {full}")
            if case_insensitive_target and rel:
                key = rel.casefold()
                prior = folded_seen.get(key)
                if prior is not None and prior != rel:
                    raise LocalWorkspaceError(
                        "Source paths only differ in case and would collide on the local "
                        f"filesystem: {os.path.join(source, prior)} vs {full}"
                    )
                folded_seen[key] = rel
            entries.append((rel, full, st))
        per_root[index] = entries

    free = shutil.disk_usage(local_base.parent).free
    required = total_bytes + LOCAL_RESERVE_BYTES
    if free < required:
        raise LocalWorkspaceError(
            f"Not enough local space: need {required:,} bytes including safety reserve, "
            f"but only {free:,} bytes are free"
        )
    return per_root, total_files, total_bytes


def _copy_regular_with_hash(source: str, destination: str, cancel_check=None) -> dict:
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    # A source entry that was a regular file during _walk_entries can be
    # swapped to a symlink or FIFO before we open it: a bare open() would then
    # follow the link (reading from outside the workspace) or block on the
    # FIFO. Re-lstat immediately before open and fstat the opened descriptor
    # so a type flip in either window aborts staging instead of quietly
    # copying the wrong bytes. Where available, O_NOFOLLOW refuses a symlink
    # swap at the syscall boundary.
    open_flags = os.O_RDONLY
    if hasattr(os, "O_BINARY"):
        open_flags |= os.O_BINARY
    if hasattr(os, "O_NOFOLLOW"):
        open_flags |= os.O_NOFOLLOW
    for attempt in range(2):
        before = os.stat(source, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise LocalWorkspaceError(
                f"Source entry is no longer a regular file and cannot be staged: {source}"
            )
        fd, tmp = tempfile.mkstemp(
            prefix=f".{os.path.basename(destination)}.vireo-copying-",
            dir=os.path.dirname(destination),
        )
        os.close(fd)
        digest = hashlib.sha256()
        source_fd = None
        try:
            try:
                source_fd = os.open(source, open_flags)
            except OSError as exc:
                raise LocalWorkspaceError(
                    f"Source entry could not be opened as a regular file: {source}"
                ) from exc
            fd_stat = os.fstat(source_fd)
            if not stat.S_ISREG(fd_stat.st_mode):
                raise LocalWorkspaceError(
                    f"Source entry is no longer a regular file and cannot be staged: {source}"
                )
            src = os.fdopen(source_fd, "rb")
            source_fd = None
            with src, open(tmp, "wb") as dst:
                while True:
                    if cancel_check and cancel_check():
                        raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                    digest.update(chunk)
            # No per-file fsync: until the activation transaction commits, a
            # crash resolves to state 'staging' and the whole tree is
            # discarded and re-staged, so individual durability buys nothing.
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
            if source_fd is not None:
                with suppress(OSError):
                    os.close(source_fd)
            with suppress(FileNotFoundError):
                os.unlink(tmp)
            raise
    raise AssertionError("copy retry loop exhausted")


def _copy_entry(source: str, destination: str, st, cancel_check=None) -> dict | None:
    mode = st.st_mode
    if stat.S_ISDIR(mode):
        os.makedirs(destination, exist_ok=True)
        return None
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


def _other_local_source_roots(db, workspace_id: int) -> list[str]:
    rows = db.conn.execute(
        "SELECT source_path FROM local_workspace_folders WHERE workspace_id != ? AND is_root = 1",
        (workspace_id,),
    ).fetchall()
    return [row["source_path"] for row in rows]


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

    shared = db.conn.execute(
        """SELECT f.path
           FROM workspace_folders current_wf
           JOIN workspace_folders other_wf
             ON other_wf.folder_id = current_wf.folder_id
            AND other_wf.workspace_id != current_wf.workspace_id
           JOIN folders f ON f.id = current_wf.folder_id
           WHERE current_wf.workspace_id = ?
           LIMIT 1""",
        (workspace_id,),
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
    # the exact-ID links above. Another active local workspace's catalog rows
    # point at its managed copy, so its claim on the original source roots
    # comes from local_workspace_folders.
    other_roots = db.conn.execute(
        """SELECT f.path
           FROM workspace_folders wf
           JOIN folders f ON f.id = wf.folder_id
           WHERE wf.workspace_id != ? AND wf.is_root = 1""",
        (workspace_id,),
    ).fetchall()
    other_root_paths = {row["path"] for row in other_roots}
    other_root_paths.update(_other_local_source_roots(db, workspace_id))
    for folder in folders:
        for other_root_path in other_root_paths:
            if _is_within(folder["path"], other_root_path) or _is_within(other_root_path, folder["path"]):
                raise LocalWorkspaceError(
                    f"Folder overlaps a root used by another workspace: {other_root_path}. "
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
                "status": folder["status"],
            }
        )
    return root_records, folder_records


def _delete_state_rows(db, workspace_id: int) -> None:
    db.conn.execute("DELETE FROM local_workspace_folders WHERE workspace_id=?", (workspace_id,))
    db.conn.execute("DELETE FROM local_workspaces WHERE workspace_id=?", (workspace_id,))


def stage_workspace(db, workspace_id: int, vireo_dir: str, *, progress=None, cancel_check=None, begin_commit=None) -> dict:
    """Copy a workspace locally and atomically rebase its catalog paths."""
    with _workspace_lock(workspace_id):
        base = workspace_dir(vireo_dir, workspace_id)
        with _STAGE_GUARD:
            if local_state(db, workspace_id):
                raise LocalWorkspaceError("This workspace is already staged locally")
            # A leftover tree without a state row is debris from a crash
            # after a completed restore; staging owns this directory.
            if base.exists():
                shutil.rmtree(base, ignore_errors=True)

            roots, folders = _root_records(db, workspace_id, base / "files")

            # Record the claim (state + folder mapping) before any copying so
            # concurrent stages and the delete-workspace guard see it, and a
            # crash during the copy resolves to a cleanable 'staging' row.
            db.conn.execute("BEGIN IMMEDIATE")
            try:
                db.conn.execute(
                    "INSERT INTO local_workspaces (workspace_id, state, created_at) VALUES (?, 'staging', ?)",
                    (workspace_id, time.time()),
                )
                root_ids = {root["folder_id"]: index for index, root in enumerate(roots)}
                for folder in folders:
                    db.conn.execute(
                        """INSERT INTO local_workspace_folders
                           (workspace_id, folder_id, source_path, local_path, original_status, is_root, root_index)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            workspace_id,
                            folder["folder_id"],
                            folder["source_path"],
                            folder["local_path"],
                            folder["status"],
                            1 if folder["folder_id"] in root_ids else 0,
                            root_ids.get(folder["folder_id"]),
                        ),
                    )
                db.conn.commit()
            except BaseException:
                db.conn.rollback()
                raise

        copied = 0
        copied_bytes = 0
        try:
            # The (slow) source enumeration happens outside the global stage
            # guard so a large workspace doesn't block other stages; a
            # failure here resolves through the 'staging' cleanup below.
            entries_per_root, total_files, total_bytes = _collect_source_entries(roots, base)
            manifest = {
                "version": MANIFEST_VERSION,
                "workspace_id": workspace_id,
                "created_at": time.time(),
                "total_files": total_files,
                "total_bytes": total_bytes,
                "roots": [
                    {"folder_id": r["folder_id"], "source_path": r["source_path"], "local_path": r["local_path"]}
                    for r in roots
                ],
                "files": [],
            }
            for root_index, root in enumerate(roots):
                os.makedirs(root["local_path"], exist_ok=True)
                for rel, source, st in entries_per_root[root_index]:
                    if cancel_check and cancel_check():
                        raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
                    destination = os.path.join(root["local_path"], rel)
                    record = _copy_entry(source, destination, st, cancel_check)
                    if record is None:
                        continue
                    record.update({"root": root_index, "path": rel})
                    manifest["files"].append(record)
                    copied += 1
                    copied_bytes += record.get("size", 0)
                    if progress:
                        progress(copied, total_files, copied_bytes, total_bytes, rel)

            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("Local workspace transfer cancelled")
            # From here on the catalog rebase may commit; the caller must stop
            # honoring cancellation or the job would report 'cancelled' for a
            # stage that actually activated.
            if begin_commit and not begin_commit():
                raise LocalWorkspaceCancelled("Local workspace transfer cancelled")

            _write_manifest(manifest_path(vireo_dir, workspace_id), manifest)
            db.conn.execute("BEGIN IMMEDIATE")
            try:
                for folder in folders:
                    db.conn.execute(
                        "UPDATE folders SET path=? WHERE id=? AND path=?",
                        (folder["local_path"], folder["folder_id"], folder["source_path"]),
                    )
                    if db.conn.execute("SELECT changes()").fetchone()[0] != 1:
                        raise LocalWorkspaceError(f"Catalog folder changed while staging: {folder['source_path']}")
                db.conn.execute(
                    "UPDATE local_workspaces SET state='active', activated_at=? WHERE workspace_id=?",
                    (time.time(), workspace_id),
                )
                db.conn.commit()
            except BaseException:
                db.conn.rollback()
                raise
            db.invalidate_new_images_cache_for_workspace(workspace_id)

            return {
                "ok": True,
                "files": total_files,
                "bytes": total_bytes,
                "local_path": str(base / "files"),
            }
        except BaseException:
            # The catalog only changes in the activation transaction; if the
            # state row still says 'staging', nothing was rebased and the
            # partial copy is safe to remove.
            current = local_state(db, workspace_id)
            if current and current.get("state") == "staging":
                shutil.rmtree(base, ignore_errors=True)
                _delete_state_rows(db, workspace_id)
                db.conn.commit()
            raise


def _entry_type(st) -> str:
    mode = st.st_mode
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISDIR(mode):
        return "dir"
    return "special"


def _same_metadata(full: str, st, baseline: dict) -> bool:
    """Size + mtime_ns comparison against the staged baseline.

    Staging preserves the source mtime on the local copy (copystat), so an
    untouched local file matches its baseline exactly; any real edit writes a
    new mtime. This is the rsync trade: an edit that deliberately restores
    both size and nanosecond mtime would be missed.
    """
    kind = _entry_type(st)
    if kind != baseline.get("type"):
        return False
    if kind == "symlink":
        return os.readlink(full) == baseline.get("target")
    return st.st_size == baseline.get("size") and st.st_mtime_ns == baseline.get("mtime_ns")


def _same_content(first: str, second: str, cancel_check=None) -> bool:
    try:
        first_st = os.lstat(first)
    except FileNotFoundError:
        first_st = None
    try:
        second_st = os.lstat(second)
    except FileNotFoundError:
        second_st = None
    if first_st is None or second_st is None:
        return first_st is None and second_st is None
    first_kind, second_kind = _entry_type(first_st), _entry_type(second_st)
    if first_kind != second_kind:
        return False
    if first_kind == "symlink":
        return os.readlink(first) == os.readlink(second)
    if first_kind != "file" or first_st.st_size != second_st.st_size:
        return False
    return _sha256(first, cancel_check) == _sha256(second, cancel_check)


def _manifest_maps(manifest: dict) -> tuple[dict, dict]:
    """Return (baseline entries, current local entries with stats)."""
    baseline = {(item["root"], item["path"]): item for item in manifest["files"]}
    local = {}
    for root_index, root in enumerate(manifest["roots"]):
        if not os.path.isdir(root["local_path"]):
            continue
        for rel, full, st in _walk_entries(root["local_path"]):
            kind = _entry_type(st)
            if kind == "dir":
                continue
            if kind == "special":
                raise LocalWorkspaceError(f"Unsupported special file in the managed local workspace: {full}")
            if kind == "symlink" and not _symlink_stays_within(full, root["local_path"]):
                raise LocalWorkspaceError(
                    f"Symlink escapes or uses an absolute target in the managed local workspace: {full}"
                )
            local[(root_index, rel)] = (full, st)
    return baseline, local


def _local_changes(manifest: dict) -> tuple[dict, dict, list, list]:
    """Return (baseline, local, changed keys, deleted keys) by metadata."""
    baseline, local = _manifest_maps(manifest)
    deleted = sorted(set(baseline) - set(local))
    changed = []
    for key, (full, st) in sorted(local.items()):
        original = baseline.get(key)
        if original is None or not _same_metadata(full, st, original):
            changed.append(key)
    return baseline, local, changed, deleted


def status(db, workspace_id: int, vireo_dir: str) -> dict:
    """Return lightweight local-workspace state and local change counts."""
    state_row = local_state(db, workspace_id)
    if not state_row:
        return {"state": "remote", "workspace_id": workspace_id}

    roots = _db_roots(db, workspace_id)
    result = {
        "state": state_row["state"],
        "workspace_id": workspace_id,
        "created_at": state_row.get("created_at"),
        "local_path": str(workspace_dir(vireo_dir, workspace_id) / "files"),
        "source_paths": [root["source_path"] for root in roots],
    }

    manifest = None
    manifest_error = None
    try:
        manifest = _load_manifest(vireo_dir, workspace_id)
    except LocalWorkspaceError as exc:
        manifest_error = str(exc)

    if manifest:
        result["total_files"] = manifest.get("total_files", 0)
        result["total_bytes"] = manifest.get("total_bytes", 0)

    if result["state"] == "staging":
        # Either a copy job is currently running (the caller reports the live
        # job alongside this payload) or the copy was interrupted and needs
        # cleanup. The catalog has not been touched either way.
        return result

    if result["state"] == "syncing":
        # A sync-back was interrupted after publishing at least one source
        # file. Finishing the sync preserves those edits; discard requires an
        # explicit acknowledgement that unpublished local changes are lost.
        result["state"] = "recovery"
        result["recovery_kind"] = "sync"
        result.update(_change_summary(manifest, manifest_error))
        return result

    missing_local_roots = [root["local_path"] for root in roots if not os.path.isdir(root["local_path"])]
    if missing_local_roots:
        result["state"] = "recovery"
        result["missing_local_paths"] = missing_local_roots
        return result

    result.update(_change_summary(manifest, manifest_error))
    result["source_available"] = all(os.path.isdir(root["source_path"]) for root in roots)
    return result


def _change_summary(manifest: dict | None, manifest_error: str | None) -> dict:
    """Compute change counts, degrading to an explanation instead of failing.

    The Workspace page must keep rendering (and keep Discard reachable) even
    when the local tree contains something sync would refuse — an escaping
    symlink, a special file, or a missing/corrupt manifest.
    """
    if manifest is None:
        return {
            "changes_error": manifest_error or "The staged file inventory is missing; sync is unavailable but Discard still restores the catalog.",
            "sync_available": False,
        }
    try:
        _baseline, _local, changed, deleted = _local_changes(manifest)
    except LocalWorkspaceError as exc:
        return {"changes_error": str(exc), "sync_available": False}
    created = sum(1 for key in changed if key not in _baseline)
    modified = len(changed) - created
    return {
        "changes": {"created": created, "modified": modified, "deleted": len(deleted)},
        "sync_available": True,
    }


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
        # Open for write so os.fsync works on Windows (its _commit backend
        # rejects read-only handles with EBADF).
        with open(temp, "rb+") as handle:
            os.fsync(handle.fileno())
        os.replace(temp, remote_path)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temp)
        raise


def _prepare_publish_target(remote_path: str) -> None:
    """Clear an empty directory tree occupying a publish target.

    A local edit can replace a tracked directory with a file of the same
    name; its baseline files appear in the deleted set (removed before
    publishes run), leaving empty directories behind. Anything non-empty
    means the source gained files we never confirmed against — refuse.
    """
    if not os.path.isdir(remote_path) or os.path.islink(remote_path):
        return
    for dirpath, _dirnames, filenames in os.walk(remote_path, topdown=False):
        if filenames:
            raise LocalWorkspaceConflict([os.path.join(dirpath, filenames[0])])
        os.rmdir(dirpath)


def _ancestor_conflicts(publish_keys, manifest: dict, deleted_set: set) -> list[str]:
    """Source paths where a non-directory blocks a new local subtree.

    Publishing ``a/b/c`` needs every ancestor of ``c`` to be a real
    directory on the source; a file or a symlink sitting at ``a/b`` would
    make the publish fail after the sync already began, or (for a symlink
    to a directory) silently redirect the write outside the workspace.
    Ancestors that the deletion pass is about to remove (a local
    file→directory replacement) are not conflicts.
    """
    conflicts = []
    checked = set()
    for root_index, rel in publish_keys:
        source_root = manifest["roots"][root_index]["source_path"]
        parent = os.path.dirname(os.path.join(source_root, rel))
        while _is_within(parent, source_root) and _norm(parent) != _norm(source_root):
            if parent in checked:
                break
            checked.add(parent)
            try:
                parent_st = os.lstat(parent)
            except FileNotFoundError:
                parent = os.path.dirname(parent)
                continue
            except OSError as exc:
                raise LocalWorkspaceError(f"Could not stat source ancestor {parent}: {exc}") from exc
            if not stat.S_ISDIR(parent_st.st_mode) and (
                root_index,
                _relative(parent, source_root),
            ) not in deleted_set:
                conflicts.append(parent)
            parent = os.path.dirname(parent)
    return conflicts


def _restore_catalog(db, workspace_id: int) -> None:
    """Atomically restore catalog paths from the recorded mapping.

    Runs as one transaction: merge any rows that re-tracked a source path
    while the workspace was staged (self-healing instead of refusing), rename
    the mapped rows back through unique temp paths (swaps/nesting can never
    trip the folders.path UNIQUE constraint midway), rebase any stray rows
    created under the managed local tree during the session, then drop the
    local-workspace state in the same commit.
    """
    mappings = _db_mappings(db, workspace_id)
    mapped_ids = {m["folder_id"] for m in mappings}
    local_roots = [m for m in mappings if m["is_root"]]

    db.conn.execute("BEGIN IMMEDIATE")
    try:
        # Self-heal: another row occupying a restore target (e.g. an import
        # or scan re-created the source path while staged) is merged into
        # the staged row instead of wedging both sync and discard.
        for mapping in mappings:
            conflict = db.conn.execute(
                "SELECT id FROM folders WHERE path=? AND id != ?",
                (mapping["source_path"], mapping["folder_id"]),
            ).fetchone()
            if conflict and conflict["id"] not in mapped_ids:
                db._merge_into_existing(conflict["id"], mapping["folder_id"], mapping["source_path"])

        for mapping in mappings:
            db.conn.execute(
                "UPDATE folders SET path=? WHERE id=?",
                (f"__vireo_local_restore__/{workspace_id}/{mapping['folder_id']}", mapping["folder_id"]),
            )
        for mapping in mappings:
            db.conn.execute(
                "UPDATE folders SET path=?, status=? WHERE id=?",
                (mapping["source_path"], mapping["original_status"], mapping["folder_id"]),
            )

        # Stray rows created under the managed local tree mid-session (e.g. a
        # scan of the local copy materialized a subfolder) rebase to the
        # matching source location; a row already there absorbs them.
        relinked = list(mapped_ids)
        rows = db.conn.execute("SELECT id, path, status FROM folders").fetchall()
        for row in rows:
            if row["id"] in mapped_ids:
                continue
            matches = [root for root in local_roots if _is_within(row["path"], root["local_path"])]
            if not matches:
                continue
            root = max(matches, key=lambda item: len(_norm(item["local_path"])))
            target = os.path.normpath(
                os.path.join(root["source_path"], _relative(row["path"], root["local_path"]))
            )
            existing = db.conn.execute(
                "SELECT id FROM folders WHERE path=? AND id != ?", (target, row["id"])
            ).fetchone()
            if existing:
                db._merge_into_existing(row["id"], existing["id"], target)
            else:
                db.conn.execute("UPDATE folders SET path=? WHERE id=?", (target, row["id"]))
                relinked.append(row["id"])

        db._relink_parents_by_path(relinked)
        _delete_state_rows(db, workspace_id)
        db.conn.commit()
    except BaseException:
        db.conn.rollback()
        raise
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def sync_back(
    db,
    workspace_id: int,
    vireo_dir: str,
    *,
    allow_deletions: bool = False,
    confirmed_deletions: int | None = None,
    progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Publish local changes, restore source paths, and remove the local copy."""
    with _workspace_lock(workspace_id):
        state_row = local_state(db, workspace_id)
        if not state_row or state_row["state"] not in {"active", "syncing"}:
            raise LocalWorkspaceError("This workspace is not working locally")
        resuming = state_row["state"] == "syncing"
        # A resumed sync inherits the deletion confirmation from the original
        # attempt, but only for the exact deletions the user confirmed then
        # — any new deletion that appeared after the interruption still needs
        # a fresh count-bound confirmation, which is validated below against
        # the persisted recovery marker OR a fresh ``confirmed_deletions``
        # count the caller sends alongside ``confirm_deletions: true``.
        recovery_confirmed: set | None = None
        if resuming:
            allow_deletions = True
            recovery_confirmed = _load_sync_recovery(vireo_dir, workspace_id)

        manifest = _load_manifest(vireo_dir, workspace_id)
        if manifest is None:
            raise LocalWorkspaceError(
                "The staged file inventory is missing, so local changes cannot be verified. "
                "Discard restores the catalog without touching source files."
            )

        for root in manifest["roots"]:
            try:
                source_st = os.lstat(root["source_path"])
            except OSError:
                raise LocalWorkspaceError(f"Source storage is unavailable: {root['source_path']}") from None
            # os.path.isdir follows symlinks; a root replaced with a symlink
            # to another directory after staging would let sync publish and
            # delete through the link into a tree outside the recorded
            # workspace. Reject any non-directory or symlinked root here.
            if stat.S_ISLNK(source_st.st_mode):
                raise LocalWorkspaceError(
                    f"Source storage changed shape after staging (now a symlink): {root['source_path']}"
                )
            if not stat.S_ISDIR(source_st.st_mode):
                raise LocalWorkspaceError(f"Source storage is unavailable: {root['source_path']}")
            if not os.path.isdir(root["local_path"]):
                raise LocalWorkspaceError(
                    f"Managed local folder is unavailable: {root['local_path']}. "
                    "Restore it or discard the local workspace; source files were not changed."
                )

        baseline, local, changed, deleted = _local_changes(manifest)
        if deleted and not allow_deletions:
            raise LocalWorkspaceError(f"Local work deleted {len(deleted)} file(s); confirm deletions before syncing")
        # Fresh count-bound check applies to both initial and resumed syncs:
        # if the caller sent a count and it no longer matches the current
        # deletion set, force the UI to re-prompt against the current facts.
        if confirmed_deletions is not None and len(deleted) > confirmed_deletions:
            raise LocalWorkspaceError(
                f"Local deletions changed since you confirmed: {len(deleted)} file(s) would now be "
                "deleted from the source. Review and confirm again."
            )
        # Resume path: any deletion that appeared after the first attempt was
        # interrupted was NOT part of the user's original count-bound
        # confirmation. A caller providing a fresh ``confirmed_deletions`` that
        # matches the current deletion count counts as re-confirming the
        # current set; the recovery marker is rewritten below so a second
        # interruption resumes against those same deletions. Without a fresh
        # count the marker gate refuses instead of silently authorizing extra
        # source deletions when "Finish Sync-back" is clicked.
        fresh_confirmation = (
            resuming and confirmed_deletions is not None and confirmed_deletions == len(deleted)
        )
        if resuming and recovery_confirmed is not None and not fresh_confirmation:
            new_deletions = [key for key in deleted if key not in recovery_confirmed]
            if new_deletions:
                raise LocalWorkspaceError(
                    f"Local deletions changed since sync was interrupted: "
                    f"{len(new_deletions)} file(s) would now be deleted from the source "
                    "that were not part of your original confirmation. Review and confirm again."
                )

        # Conflict scan. Only paths sync would write or delete can clobber
        # anything, so only those are verified — by full content hash, since
        # a source edit can preserve size and mtime. Unchanged local files
        # are never published, so a source-side edit to them simply survives.
        conflicts = []
        at_risk = [key for key in changed if key in baseline] + list(deleted)
        for key in at_risk:
            root_index, rel = key
            original = baseline[key]
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            remote_matches, remote_sha = _source_state(remote_path, original, cancel_check)
            if remote_matches:
                continue
            entry = local.get(key)
            local_path = entry[0] if entry else None
            # A prior interrupted sync may already have published this exact
            # result. Treat it as resumable, not as an outside conflict.
            if local_path is None and not os.path.lexists(remote_path):
                continue
            if local_path and _matches_remote(local_path, remote_path, remote_sha, cancel_check):
                continue
            conflicts.append(remote_path)

        deleted_set = set(deleted)
        for key in changed:
            if key in baseline:
                continue
            root_index, rel = key
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            if not os.path.lexists(remote_path):
                continue
            if os.path.isdir(remote_path) and not os.path.islink(remote_path):
                # A local edit replaced this directory with a file. That is
                # only safe if the deletions about to run empty the source
                # directory completely; anything else there is outside work.
                conflicts.extend(
                    full
                    for entry_rel, full, st in _walk_entries(remote_path)
                    if not stat.S_ISDIR(st.st_mode)
                    and (root_index, os.path.join(rel, entry_rel)) not in deleted_set
                )
            elif not _same_content(local[key][0], remote_path, cancel_check):
                conflicts.append(remote_path)

        conflicts.extend(_ancestor_conflicts(changed, manifest, deleted_set))
        if conflicts:
            raise LocalWorkspaceConflict(sorted(set(conflicts)))

        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Local workspace sync cancelled")
        if begin_commit and not begin_commit():
            raise LocalWorkspaceCancelled("Local workspace sync cancelled")

        # Persist the recovery boundary before the first source mutation, so
        # an interruption is recognized as a partially-published sync. The
        # recovery marker records the confirmed deletion set so a resumed
        # sync cannot silently authorize additional deletions that appeared
        # after the first attempt was interrupted. On a fresh count-bound
        # confirmation during resume, rewrite the marker so a second
        # interruption resumes against the newly-confirmed set instead of the
        # original (now smaller) one.
        if not resuming:
            _write_sync_recovery(vireo_dir, workspace_id, deleted)
            db.conn.execute(
                "UPDATE local_workspaces SET state='syncing' WHERE workspace_id=?", (workspace_id,)
            )
            db.conn.commit()
        elif fresh_confirmation:
            _write_sync_recovery(vireo_dir, workspace_id, deleted)

        total = len(changed) + len(deleted)
        done = 0
        # Deletions first: a local rename of a directory to a file (or vice
        # versa) needs the old source entries gone before publishes create
        # their replacements.
        for root_index, rel in deleted:
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            with suppress(FileNotFoundError):
                os.unlink(remote_path)
            done += 1
            if progress:
                progress(done, total, rel)
        for key in changed:
            root_index, rel = key
            remote_path = os.path.join(manifest["roots"][root_index]["source_path"], rel)
            _prepare_publish_target(remote_path)
            _atomic_publish(local[key][0], remote_path)
            done += 1
            if progress:
                progress(done, total, rel)

        _restore_catalog(db, workspace_id)
        shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
        return {
            "ok": True,
            "created_or_modified": len(changed),
            "deleted": len(deleted),
            "files_examined": len(local),
        }


def _source_state(remote_path: str, baseline: dict, cancel_check=None) -> tuple[bool, str | None]:
    """Compare a source entry against the staged baseline by content.

    Returns (matches, remote sha256 or None). The hash is computed at most
    once and handed back so callers never re-read the remote file.
    """
    try:
        st = os.lstat(remote_path)
    except FileNotFoundError:
        return False, None
    kind = _entry_type(st)
    if kind != baseline.get("type"):
        return False, None
    if kind == "symlink":
        return os.readlink(remote_path) == baseline.get("target"), None
    if st.st_size != baseline.get("size"):
        return False, None
    remote_sha = _sha256(remote_path, cancel_check)
    return remote_sha == baseline.get("sha256"), remote_sha


def _matches_remote(local_path: str, remote_path: str, remote_sha: str | None, cancel_check=None) -> bool:
    """Check whether the local entry equals the source entry, reusing the
    remote hash computed by the conflict scan when available."""
    if remote_sha is None:
        return _same_content(local_path, remote_path, cancel_check)
    try:
        st = os.lstat(local_path)
    except FileNotFoundError:
        return False
    if _entry_type(st) != "file":
        return False
    return _sha256(local_path, cancel_check) == remote_sha


def discard_local(db, workspace_id: int, vireo_dir: str, *, acknowledge_published: bool = False) -> dict:
    """Restore catalog paths and remove local changes without touching source files."""
    with _workspace_lock(workspace_id):
        state_row = local_state(db, workspace_id)
        if not state_row:
            raise LocalWorkspaceError("This workspace is not working locally")
        state = state_row["state"]
        if state == "staging":
            shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
            _delete_state_rows(db, workspace_id)
            db.conn.commit()
            return {"ok": True, "discarded": True}
        if state == "syncing" and not acknowledge_published:
            raise LocalWorkspaceError(
                "A sync-back was interrupted after some files were already published to the source. "
                "Finish the sync-back to preserve those edits, or discard again acknowledging that "
                "unpublished local changes will be lost."
            )
        if state not in {"active", "syncing"}:
            raise LocalWorkspaceError("Local workspace is not in a recoverable state")

        _restore_catalog(db, workspace_id)
        shutil.rmtree(workspace_dir(vireo_dir, workspace_id), ignore_errors=True)
        return {"ok": True, "discarded": True}
