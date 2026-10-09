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
    destination_disk_space,
    source_tree_size,
    stage_boundary_lock,
)

# Job types of the folder-scoped stage/sync/discard transitions. The folder
# status endpoint reports them, and folder moves refuse to race one.
LOCAL_FOLDER_JOB_TYPES = frozenset(
    {"work-locally-folder-stage", "work-locally-folder-sync", "work-locally-folder-discard"}
)

_LOCKS: dict[int, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


def _folder_lock(root_folder_id: int) -> threading.RLock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(int(root_folder_id), threading.RLock())


def folder_dir(vireo_dir: str, root_folder_id: int) -> Path:
    return Path(vireo_dir) / "local-folders" / str(int(root_folder_id))


def default_local_base(vireo_dir: str, root_folder_id: int) -> Path:
    """Return the parent directory used for a folder's managed local copy."""
    return folder_dir(vireo_dir, root_folder_id) / "files"


def _safe_folder_name(source_path: str, root_folder_id: int) -> str:
    name = Path(source_path.rstrip("/\\")).name or f"folder-{root_folder_id}"
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)


def default_local_path(vireo_dir: str, root_folder_id: int, source_path: str) -> Path:
    return local_path_for_base(
        default_local_base(vireo_dir, root_folder_id), root_folder_id, source_path
    )


def local_path_for_base(local_base: str | Path, root_folder_id: int, source_path: str) -> Path:
    return Path(local_base) / _safe_folder_name(source_path, root_folder_id)


def local_copy_preflight(
    db,
    root_folder_ids: list[int],
    vireo_dir: str,
    *,
    destination_bases: dict[int, str] | None = None,
    cancel_check=None,
) -> dict:
    """Measure selected sources and capacity on their destination volumes."""
    destination_bases = destination_bases or {}
    folders = []
    volumes_by_device: dict[int, dict] = {}

    for raw_root_id in root_folder_ids:
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Folder scan cancelled")
        root_folder_id = int(raw_root_id)
        row = db.get_folder(root_folder_id)
        if row is None:
            raise LocalWorkspaceError(f"Folder {root_folder_id} was not found")
        source_path = row["path"]
        local_base = destination_bases.get(root_folder_id)
        if local_base is None:
            local_base = str(default_local_base(vireo_dir, root_folder_id))
        local_path = local_path_for_base(local_base, root_folder_id, source_path)
        total_files, total_bytes, estimated_bytes = source_tree_size(
            source_path, cancel_check=cancel_check
        )
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Folder scan cancelled")
        space = destination_disk_space(local_path, cancel_check=cancel_check)
        device = int(space["device"])
        volume = volumes_by_device.setdefault(
            device,
            {
                "device": device,
                "free_bytes": int(space["free_bytes"]),
                "total_bytes": int(space["total_bytes"]),
                "reserve_bytes": int(space["reserve_bytes"]),
                "copy_bytes": 0,
                "destination_paths": [],
                "folder_ids": [],
            },
        )
        # If free space changes during a multi-folder scan, use the most
        # conservative reading for the shared-volume aggregate.
        volume["free_bytes"] = min(volume["free_bytes"], int(space["free_bytes"]))
        volume["copy_bytes"] += estimated_bytes
        volume["destination_paths"].append(str(local_path))
        volume["folder_ids"].append(root_folder_id)
        folders.append(
            {
                "folder_id": root_folder_id,
                "source_path": source_path,
                "destination_path": str(local_path),
                "total_files": total_files,
                "total_bytes": total_bytes,
                "estimated_bytes": estimated_bytes,
                "device": device,
            }
        )

    volumes = []
    for index, volume in enumerate(volumes_by_device.values(), start=1):
        volume["id"] = index
        volume["required_bytes"] = volume["copy_bytes"] + volume["reserve_bytes"]
        volume["after_copy_bytes"] = volume["free_bytes"] - volume["copy_bytes"]
        volume["can_copy"] = volume["free_bytes"] >= volume["required_bytes"]
        volumes.append(volume)
        for folder in folders:
            if folder.get("device") == volume["device"]:
                folder["volume_id"] = volume["id"]
                folder["free_bytes"] = volume["free_bytes"]
                folder["reserve_bytes"] = volume["reserve_bytes"]
    for folder in folders:
        folder.pop("device", None)
    for volume in volumes:
        volume.pop("device", None)

    return {
        "folder_count": len(folders),
        "total_bytes": sum(folder["total_bytes"] for folder in folders),
        "can_copy": all(volume["can_copy"] for volume in volumes),
        "folders": folders,
        "volumes": volumes,
    }


def manifest_path(vireo_dir: str, root_folder_id: int) -> Path:
    return folder_dir(vireo_dir, root_folder_id) / "manifest.json"


def _sync_recovery_path(vireo_dir: str, root_folder_id: int) -> Path:
    return folder_dir(vireo_dir, root_folder_id) / "sync-recovery.json"


def _remove_tree(path: Path) -> None:
    """Remove a managed tree without ever following a replacement symlink."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return
    try:
        if stat.S_ISLNK(st.st_mode):
            os.unlink(path)
        elif stat.S_ISDIR(st.st_mode):
            shutil.rmtree(path)
        else:
            os.unlink(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise LocalWorkspaceError(f"Could not remove local data at {path}: {exc}") from exc


def _remove_folder_dir(
    vireo_dir: str, root_folder_id: int, local_path: str | None = None
) -> None:
    """Remove session metadata and its copy, including a custom destination."""
    base = folder_dir(vireo_dir, root_folder_id)
    if local_path:
        local_root = Path(local_path)
        try:
            inside_session = _physical_is_within(str(local_root), str(base))
        except OSError:
            inside_session = False
        if not inside_session:
            _remove_tree(local_root)
    _remove_tree(base)


def folder_state(db, root_folder_id: int) -> dict | None:
    row = db.local_folders.get_state(root_folder_id)
    return dict(row) if row else None


def _mappings(db, root_folder_id: int) -> list[dict]:
    rows = db.local_folders.mappings(root_folder_id)
    return [dict(row) for row in rows]


def _root_mapping(db, root_folder_id: int) -> dict | None:
    row = db.local_folders.root_mapping(root_folder_id)
    return dict(row) if row else None


def local_root_for_folder(db, folder_id: int) -> int | None:
    """Return the local-session root covering ``folder_id``, if any."""
    row = db.local_folders.root_for_folder(folder_id)
    return int(row["root_folder_id"]) if row else None


def local_roots_under_folder(db, folder_id: int) -> list[int]:
    """Return every local-session root whose source lives inside ``folder_id``.

    Staging rebases the child folder row's ``folders.path`` under
    ``local-folders/``, so a subtree scan on ``folders.path`` (as
    ``delete_folder``/``relocate_folder`` do) no longer sees it — while
    ``local_folder_mappings.source_path`` still records the original
    location beneath the ancestor. Callers that must consider every
    affected descendant session (workspace status aggregation, ancestor
    unlink cleanup) use this; guards that only need to refuse on any
    match use :func:`local_root_under_folder` for a cheap short-circuit.
    """
    row = db.get_folder(folder_id)
    if row is None or not row["path"]:
        return []
    folder_path = row["path"]
    result: set[int] = set()
    for entry in db.local_folders.source_roots():
        source = entry["source_path"]
        if not source or source == folder_path:
            continue
        if _is_within(source, folder_path):
            result.add(int(entry["root_folder_id"]))
    return sorted(result)


def local_root_under_folder(db, folder_id: int) -> int | None:
    """Return one local-session root whose source lives inside ``folder_id``.

    Short-circuit variant of :func:`local_roots_under_folder`; guards that
    only need to know whether *any* descendant session exists (delete,
    relocate, move) use this to refuse with 409 instead of enumerating
    every match.
    """
    roots = local_roots_under_folder(db, folder_id)
    return roots[0] if roots else None


def _resolve_physical(path: str) -> str | None:
    """Return ``os.path.normcase(realpath(path))`` or ``None`` on failure."""
    try:
        return os.path.normcase(os.path.realpath(path))
    except (ValueError, OSError):
        return None


def _load_staged_source_index(db) -> list[tuple[int, str, str | None]]:
    """Snapshot of ``(root_folder_id, source_path, physical_source_path)``.

    Cached per call so a snapshot import that passes every frozen file path
    can compare each one against a resolved copy of every mapping without
    repeating the DB query or the ``realpath`` walk.
    """
    entries: list[tuple[int, str, str | None]] = []
    for row in db.local_folders.ordered_source_roots():
        source = row["source_path"]
        if not source:
            continue
        entries.append((int(row["root_folder_id"]), source, _resolve_physical(source)))
    return entries


def _path_overlaps_source(
    path: str,
    path_physical: str | None,
    source: str,
    source_physical: str | None,
    *,
    include_descendants: bool,
) -> bool:
    """Whether ``path`` overlaps ``source`` lexically or physically."""
    if _is_within(path, source):
        return True
    if path_physical and source_physical:
        try:
            if os.path.commonpath([path_physical, source_physical]) == source_physical:
                return True
        except ValueError:
            pass
    if not include_descendants:
        return False
    if _is_within(source, path):
        return True
    if path_physical and source_physical:
        try:
            if os.path.commonpath([source_physical, path_physical]) == path_physical:
                return True
        except ValueError:
            pass
    return False


def _staged_root_visible_to_workspace(
    db, root_folder_id: int, workspace_id: int | None,
) -> bool:
    """Whether a staged root's mappings sit under a folder the workspace sees."""
    if workspace_id is None:
        return False
    row = db.local_folders.workspace_has_root(workspace_id, root_folder_id)
    return row is not None


def local_root_overlapping_path(
    db, path: str, *, include_descendants: bool = True,
) -> int | None:
    """Return a local-session root whose original source overlaps ``path``.

    Staging rebases the session's ``folders.path`` rows under
    ``local-folders/`` while the originals stay at
    ``local_folder_mappings.source_path``. A scan that walks any part of
    that source tree -- the source itself, a directory inside it, or an
    ancestor that contains it -- therefore finds files the catalog only
    knows by their local path and adds a second row for each. Callers that
    are about to scan (or copy into) ``path`` use this to refuse instead.

    ``include_descendants=False`` ignores sessions that sit strictly below
    ``path`` -- for callers that only write into a subtree they choose
    later (an import's dated destination folders), where refusing every
    import into an archive because one old day folder is staged would be
    too broad.

    Both a lexical and a ``realpath`` (physical) comparison run: the
    lexical check catches spellings the catalog uses even when either side
    no longer resolves on disk, and the physical check catches symlink
    aliases of a staged source that would otherwise slip past the lexical
    guard and let the scanner re-catalog the originals through the alias.
    """
    if not path:
        return None
    path_physical = _resolve_physical(path)
    for root_id, source, source_physical in _load_staged_source_index(db):
        if _path_overlaps_source(
            path, path_physical, source, source_physical,
            include_descendants=include_descendants,
        ):
            return root_id
    return None


_PENDING_STAGE_STATUSES = frozenset({"queued", "running", "pausing", "paused"})


class PendingStagePath(str):
    """A path a live folder-stage job will claim, tagged with that job.

    It is the plain path everywhere a path is expected; ``workspace_id`` and
    ``status`` of the claiming job let :func:`local_copy_scan_conflict` say
    whose job it is and what state it is in.
    """

    def __new__(cls, path: str, *, workspace_id=None, status=None):
        obj = super().__new__(cls, path)
        obj.workspace_id = workspace_id
        obj.status = status
        return obj


def _pending_stage_refusal(path: str, pending, active_workspace_id) -> str:
    """Refusal naming whose folder-stage job overlaps ``path`` and its state.

    The job can belong to the caller's own workspace as easily as to
    another, and it can be queued, running or paused; the message says
    which. A plain-string entry (no job details) gets a neutral sentence.
    """
    job_ws = getattr(pending, "workspace_id", None)
    status = getattr(pending, "status", None)
    job = (
        f"a {status} folder-stage job" if status in _PENDING_STAGE_STATUSES
        else "a folder-stage job"
    )
    try:
        own = int(job_ws) == int(active_workspace_id)
    except (TypeError, ValueError):
        own = None  # owner unknown: say nothing about it
    if own is None:
        clause = f"{job} overlaps it"
    elif own:
        clause = f"this workspace has {job} that overlaps it"
    else:
        clause = f"another workspace has {job} that overlaps it"
    if status in {"pausing", "paused"}:
        advice = (
            "Resume that stage job and let it finish, or cancel it, "
            "before scanning."
        )
    else:
        advice = "Wait for that stage job to finish before scanning."
    return f"Cannot scan {path}: {clause}. {advice}"


def stage_pending_source_paths(list_jobs, db) -> list[str]:
    """Paths reserved by queued/running folder-stage jobs.

    A stage route registers a job under
    ``config.root_folder_ids``/``config.destination_paths`` and returns
    202 before the worker enters :func:`stage_folder`, which is what
    actually creates the ``local_folder_mappings`` row. In that window a
    scan or import admission that only reads ``local_folder_mappings``
    would let a path the queued stage will rebase (its source) or write
    into (its chosen local destination) slip through. Callers pass these
    reserved paths to :func:`local_copy_scan_conflict` alongside the DB
    check so a scan or import cannot be registered against a path
    another workspace is about to make catalog-unsafe.

    Sources come from ``folders.path`` for each ``root_folder_ids``
    entry (equal to the source before staging rebases it).
    Destinations come from ``config.destination_paths`` as recorded at
    stage-registration time -- the same absolute paths ``stage_folder``
    will create on disk. Both are returned in one list because a caller
    only needs to know "which paths a queued stage will claim".

    ``list_jobs`` is the runner's ``list_jobs`` callable, or an already
    materialized job list.

    Each entry is a :class:`PendingStagePath` carrying the claiming job's
    ``workspace_id`` and ``status``. The job may be the caller's own
    workspace's, so the refusal built from it must not assume "another
    workspace" or "queued".
    """
    if list_jobs is None:
        return []
    jobs = list_jobs() if callable(list_jobs) else list_jobs
    sources: list[str] = []
    destination_paths: list[str] = []
    seen_sources: set[tuple[str, object, object]] = set()
    folder_paths: dict[int, str | None] = {}
    for job in jobs or []:
        if job.get("type") != "work-locally-folder-stage":
            continue
        status = job.get("status")
        if status not in _PENDING_STAGE_STATUSES:
            continue
        config = job.get("config") or {}
        if not isinstance(config, dict):
            continue
        job_ws = job.get("workspace_id")
        root_ids: set[int] = set()
        for raw in config.get("root_folder_ids") or []:
            try:
                root_ids.add(int(raw))
            except (TypeError, ValueError):
                continue
        for root_id in sorted(root_ids):
            if root_id not in folder_paths:
                row = db.get_folder(root_id)
                folder_paths[root_id] = row["path"] if row and row["path"] else None
            path = folder_paths[root_id]
            if path and (path, job_ws, status) not in seen_sources:
                seen_sources.add((path, job_ws, status))
                sources.append(
                    PendingStagePath(path, workspace_id=job_ws, status=status)
                )
        for raw in config.get("destination_paths") or []:
            if isinstance(raw, str) and raw:
                destination_paths.append(
                    PendingStagePath(raw, workspace_id=job_ws, status=status)
                )
    return sources + destination_paths


def local_copy_scan_conflict(
    db,
    paths,
    *,
    include_descendants: bool = True,
    active_workspace_id: int | None = None,
    pending_stage_sources: list[str] | None = None,
) -> str | None:
    """User-facing refusal when scanning ``paths`` would re-add staged photos.

    Loads the staged-source index once and reuses each source's resolved
    physical spelling across every candidate ``path``, so a snapshot
    import passing thousands of frozen file paths does one DB query and
    one ``realpath`` per staged source instead of one per path.

    When the matching mapping is not visible from ``active_workspace_id``
    (another workspace staged it and this workspace shares no folder
    with that mapping), the refusal names only the caller's path -- not
    the mapping's ``source_path``, which the caller could not otherwise
    see. Same for a queued folder-stage that overlaps: the caller is
    told a stage job overlaps their path, not which source it names.

    ``pending_stage_sources`` covers live folder-stage jobs (from any
    workspace, this one included) whose worker has not yet created the
    mapping row -- otherwise the DB check alone would accept the path in
    that window. Entries from :func:`stage_pending_source_paths` carry the
    job's workspace and status, and the refusal states both.
    """
    staged = _load_staged_source_index(db)
    pending: list[str] = []
    for source in pending_stage_sources or []:
        if source:
            pending.append(source)
    pending_physical = [(source, _resolve_physical(source)) for source in pending]
    for path in paths:
        if not path:
            continue
        path_physical = _resolve_physical(path)
        for root_id, source, source_physical in staged:
            if _path_overlaps_source(
                path, path_physical, source, source_physical,
                include_descendants=include_descendants,
            ):
                if _staged_root_visible_to_workspace(
                    db, root_id, active_workspace_id
                ):
                    return (
                        f"Cannot scan {path} while {source} has a local copy: "
                        "the scan would catalog its originals a second time. "
                        "Sync or discard the local copy first."
                    )
                return (
                    f"Cannot scan {path}: it overlaps a local copy staged "
                    "from another workspace. Sync or discard that local "
                    "copy first."
                )
        for source, source_physical in pending_physical:
            if _path_overlaps_source(
                path, path_physical, source, source_physical,
                include_descendants=include_descendants,
            ):
                return _pending_stage_refusal(
                    path, source, active_workspace_id,
                )
    return None


def folder_has_local_copy(db, folder_id: int) -> bool:
    return local_root_for_folder(db, folder_id) is not None


def workspace_local_root_ids(db, workspace_id: int) -> list[int]:
    rows = db.local_folders.workspace_root_ids(workspace_id)
    return [int(row["root_folder_id"]) for row in rows]


def _workspace_local_session_photo_count(
    db, workspace_id: int, root_folder_id: int
) -> int:
    """Count workspace-visible photos covered by one local session."""
    row = db.local_folders.visible_photo_count(workspace_id, root_folder_id)
    return int(row["photo_count"] or 0)


def workspace_has_local_folders(db, workspace_id: int) -> bool:
    return bool(workspace_local_root_ids(db, workspace_id))


def affected_workspace_ids(db, root_folder_id: int) -> list[int]:
    rows = db.local_folders.affected_workspace_ids(root_folder_id)
    return [int(row["workspace_id"]) for row in rows]


def workspace_ids_for_folder_tree(db, root_folder_id: int) -> list[int]:
    """Return every workspace whose catalog scope intersects this root.

    Unlike :func:`affected_workspace_ids`, this also works before the local
    mapping rows are inserted, so stage validation can see jobs running in a
    different workspace that shares the source folder.
    """
    root = db.get_folder(root_folder_id)
    if root is None:
        return []
    workspace_ids = set()
    rows = db.workspace_folders.all_linked_paths()
    for row in rows:
        # Intersection is symmetric: a workspace root that contains the local
        # root shares the same catalog subtree as a workspace root nested
        # inside it. Missing the ancestor direction here would let
        # _busy_job/_pending_local_workspace_transition enqueue overlapping
        # jobs in an ancestor-linked workspace during the staging window.
        if _is_within(row["path"], root["path"]) or _is_within(root["path"], row["path"]):
            workspace_ids.add(int(row["workspace_id"]))
    return sorted(workspace_ids)


def _invalidate_new_images_for_source(
    db, source_path: str | None, workspace_ids=(),
) -> None:
    """Drop cached New Images answers that a staged source changes.

    The New Images walk leaves every staged source out (see
    ``new_images.staged_source_paths``), so staging, a failed stage, a sync
    and a discard each change the answer for every workspace whose folders
    reach ``source_path`` -- including an ancestor-linked workspace that
    never linked the staged folder itself, which ``affected_workspace_ids``
    misses. ``workspace_ids`` adds the workspaces linked to the mapped rows.
    """
    ids = {int(workspace_id) for workspace_id in workspace_ids}
    if source_path:
        # A source transition can change any workspace reached through an
        # alias. Cache invalidation must never inspect the source: discard
        # works while the original share is offline. Conservatively drop
        # all linked workspace snapshots instead of resolving filesystem
        # paths after the catalog commit.
        ids.update(int(row["workspace_id"]) for row in db.workspace_folders.all_linked_workspace_ids())
    for workspace_id in sorted(ids):
        db.invalidate_new_images_cache_for_workspace(workspace_id)


def workspace_summaries_for_ids(db, workspace_ids: list[int]) -> list[dict]:
    """Return lightweight workspace details in the same order as the IDs."""
    if not workspace_ids:
        return []
    rows = db.workspaces.summaries_for_ids(workspace_ids)
    workspaces_by_id = {
        int(row["id"]): {"id": int(row["id"]), "name": row["name"]}
        for row in rows
    }
    return [
        workspaces_by_id[workspace_id]
        for workspace_id in workspace_ids
        if workspace_id in workspaces_by_id
    ]


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


def _catalog_records(
    db, root_folder_id: int, local_base: Path, vireo_dir: str
) -> tuple[list[dict], list[dict]]:
    row = db.get_folder(root_folder_id)
    if row is None:
        raise LocalWorkspaceError("Folder not found")
    source_path = row["path"]

    # A folder already covered by a broader local root simply shares that
    # copy; callers should use the covering session instead of creating an
    # overlapping tree.  A requested broader root over an existing narrower
    # session must be resolved first because two manifests cannot safely own
    # the same catalog rows.
    for existing in db.local_folders.source_roots():
        if _is_within(source_path, existing["source_path"]) or _is_within(
            existing["source_path"], source_path
        ):
            raise LocalWorkspaceError(
                f"Folder overlaps an existing local copy: {existing['source_path']}"
            )

    # Do not overlap a legacy v0.24 workspace session. It remains usable for
    # sync/discard, but new folder sessions wait until it is resolved.
    for existing in db.local_workspaces.source_roots():
        if _is_within(source_path, existing["source_path"]) or _is_within(
            existing["source_path"], source_path
        ):
            raise LocalWorkspaceError(
                "Folder is part of an older workspace-local session. "
                "Finish or discard that session before staging this folder."
            )

    local_root = local_base / _safe_folder_name(source_path, root_folder_id)
    if _physical_is_within(str(local_root), source_path) or _physical_is_within(
        source_path, str(local_root)
    ):
        raise LocalWorkspaceError(
            "Managed local storage overlaps the source folder; move Vireo's data directory to local storage first"
        )
    catalog_source_paths = {
        row["path"]
        for row in db.local_folders.catalog_paths()
        if row["path"]
    }
    catalog_source_paths.update(
        row["source_path"]
        for row in db.local_folders.source_paths()
        if row["source_path"]
    )
    catalog_source_paths.update(
        row["source_path"]
        for row in db.local_workspaces.source_paths()
        if row["source_path"]
    )
    for catalog_source in catalog_source_paths:
        if catalog_source == source_path:
            continue
        if _physical_is_within(str(local_root), catalog_source) or _physical_is_within(
            catalog_source, str(local_root)
        ):
            raise LocalWorkspaceError(
                f"Local destination overlaps a folder Vireo already manages: {catalog_source}"
            )
    protected_session_roots = [
        Path(vireo_dir) / "local-folders",
        Path(vireo_dir) / "local-workspaces",
    ]
    for session_root in protected_session_roots:
        overlaps = _physical_is_within(
            str(local_root), str(session_root)
        ) or _physical_is_within(str(session_root), str(local_root))
        belongs_to_current_files = _physical_is_within(
            str(local_root), str(default_local_base(vireo_dir, root_folder_id))
        )
        if overlaps and not belongs_to_current_files:
            raise LocalWorkspaceError(
                f"Local destination overlaps Vireo session storage: {session_root}"
            )
    for existing in db.local_folders.local_roots():
        if _physical_is_within(str(local_root), existing["local_path"]) or _physical_is_within(
            existing["local_path"], str(local_root)
        ):
            raise LocalWorkspaceError(
                f"Local destination overlaps an existing local copy: {existing['local_path']}"
            )
    if os.path.lexists(local_root):
        raise LocalWorkspaceError(
            f"Local destination already exists: {local_root}. Choose another location or rename the existing folder."
        )

    root = {
        "folder_id": int(row["id"]),
        "source_path": source_path,
        "local_path": str(local_root),
    }
    folders = []
    for folder in db.local_folders.catalog_rows_by_path():
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
    db.local_folders.delete_mappings(root_folder_id)
    db.local_folders.delete_state(root_folder_id)


def _materialize_ancestor_workspaces(db, source_path: str, folders: list[dict]) -> None:
    """Insert workspace_folders rows for ancestor-linked workspaces.

    When workspace A links ``/parent`` and hasn't yet materialized
    ``/parent/child``, staging ``/parent/child`` in another workspace rebases
    the child ``folders.path`` under ``local-folders/``. After that move,
    ``_materialize_workspace_descendants(A)`` walks ``folders.path`` below
    ``/parent`` and no longer finds the child, so A never gets a
    ``workspace_folders`` link to it. That link is what
    :func:`affected_workspace_ids` and :func:`workspace_local_root_ids` both
    key off — without it, ``workspace_status(A)`` reports ``/parent`` as
    remote and the UI hides sync/discard controls even though A's catalog
    is partly rebased under the shared local copy. Running this before the
    rebase transaction closes the gap while the child rows are still at
    their original source paths.
    """
    if not folders:
        return
    rows = db.workspace_folders.distinct_linked_paths()
    ancestor_ws_ids = sorted({
        int(row["workspace_id"])
        for row in rows
        if row["path"] and _is_within(source_path, row["path"])
    })
    if not ancestor_ws_ids:
        return
    folder_ids = [folder["folder_id"] for folder in folders]
    pairs = [(ws_id, folder_id) for ws_id in ancestor_ws_ids for folder_id in folder_ids]
    # Staging in another workspace is automatic discovery, not an explicit
    # restore. Check removals in the INSERT so even a concurrent unlink
    # cannot be undone by clearing its removal record in the link trigger.
    db.workspace_folders.materialize_local_copy_links(pairs)
    db.commit()
    for ws_id in ancestor_ws_ids:
        db._new_images_cache.invalidate_workspaces(db._db_path, [ws_id])


def stage_folder(
    db,
    root_folder_id: int,
    vireo_dir: str,
    *,
    local_base: str | None = None,
    progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Copy one top-level folder locally and atomically rebase the catalog."""
    root_folder_id = int(root_folder_id)
    with _folder_lock(root_folder_id):
        with stage_boundary_lock():
            covering = local_root_for_folder(db, root_folder_id)
            if covering is not None:
                if covering == root_folder_id:
                    raise LocalWorkspaceError("This folder is already staged locally")
                raise LocalWorkspaceError(f"This folder is already covered by local folder {covering}")
            _remove_folder_dir(vireo_dir, root_folder_id)
            selected_base = Path(local_base).expanduser() if local_base else default_local_base(
                vireo_dir, root_folder_id
            )
            if not selected_base.is_absolute():
                raise LocalWorkspaceError("Local destination must be an absolute path")
            roots, folders = _catalog_records(
                db, root_folder_id, selected_base, vireo_dir
            )
            local_root = roots[0]["local_path"]
            try:
                os.makedirs(local_root, exist_ok=False)
            except FileExistsError:
                raise LocalWorkspaceError(
                    f"Local destination already exists: {local_root}. "
                    "Choose another location or rename the existing folder."
                ) from None
            except OSError as exc:
                raise LocalWorkspaceError(
                    f"Could not create the local destination {local_root}: {exc}"
                ) from exc
            try:
                db.begin_immediate()
                db.local_folders.create_staging(root_folder_id, time.time())
                for folder in folders:
                    db.local_folders.add_mapping(root_folder_id, folder)
                db.commit()
            except BaseException:
                db.rollback()
                _remove_folder_dir(vireo_dir, root_folder_id, local_root)
                raise
            # From here the New Images walk leaves the source out.
            _invalidate_new_images_for_source(db, roots[0]["source_path"])

        copied = 0
        copied_bytes = 0
        try:
            entries_per_root, total_files, total_bytes = _collect_source_entries(
                roots, Path(roots[0]["local_path"])
            )
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

            # Ancestor-linked workspaces need explicit workspace_folders rows
            # for every descendant we're about to rebase; otherwise the rebase
            # hides those rows from _materialize_workspace_descendants and the
            # workspace can never reach the shared local copy from its
            # ancestor root. Must run before the UPDATE below.
            _materialize_ancestor_workspaces(db, roots[0]["source_path"], folders)

            db.begin_immediate()
            try:
                for folder in folders:
                    db.local_folders.rebase_folder_if_unchanged(folder)
                    if db.local_folders.last_change_count() != 1:
                        raise LocalWorkspaceError(
                            f"Catalog folder changed while staging: {folder['source_path']}"
                        )
                db.local_folders.activate(root_folder_id, time.time())
                db.commit()
            except BaseException:
                db.rollback()
                raise
            _invalidate_new_images_for_source(
                db, roots[0]["source_path"],
                affected_workspace_ids(db, root_folder_id),
            )
            return {
                "ok": True,
                "root_folder_id": root_folder_id,
                "files": total_files,
                "bytes": total_bytes,
                "local_path": roots[0]["local_path"],
            }
        except BaseException:
            current = folder_state(db, root_folder_id)
            if current and current.get("state") == "staging":
                root = _root_mapping(db, root_folder_id)
                _remove_folder_dir(
                    vireo_dir, root_folder_id, root["local_path"] if root else None
                )
                _delete_state_rows(db, root_folder_id)
                db.commit()
                # The source is no longer staged: walks cached while it
                # was must not keep leaving it out.
                _invalidate_new_images_for_source(db, roots[0]["source_path"])
            raise


def folder_status(db, root_folder_id: int, vireo_dir: str) -> dict:
    """Return status for a root, resolving a shared covering session."""
    root_folder_id = int(root_folder_id)
    covering = local_root_for_folder(db, root_folder_id)
    if covering is None:
        row = db.get_folder(root_folder_id)
        source_path = row["path"] if row else None
        workspace_ids = workspace_ids_for_folder_tree(db, root_folder_id)
        return {
            "state": "remote",
            "folder_id": root_folder_id,
            "root_folder_id": root_folder_id,
            "source_path": source_path,
            "default_local_base": str(default_local_base(vireo_dir, root_folder_id)),
            "local_folder_name": (
                _safe_folder_name(source_path, root_folder_id) if source_path else None
            ),
            "default_local_path": (
                str(default_local_path(vireo_dir, root_folder_id, source_path))
                if source_path
                else None
            ),
            "workspace_ids": workspace_ids,
        }
    root_folder_id = covering
    state_row = folder_state(db, root_folder_id)
    root = _root_mapping(db, root_folder_id)
    if state_row is None or root is None:
        raise LocalWorkspaceError("Local folder state is incomplete")
    workspace_ids = affected_workspace_ids(db, root_folder_id)
    result = {
        "state": state_row["state"],
        "folder_id": root_folder_id,
        "root_folder_id": root_folder_id,
        "source_path": root["source_path"],
        "local_path": root["local_path"],
        "created_at": state_row.get("created_at"),
        "workspace_ids": workspace_ids,
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
        status["folder_name"] = root.get("name") or _derive_folder_name(status["display_path"])
        status["visible_ancestor_folder_id"] = _nearest_visible_ancestor(
            db, int(root["id"]), workspace_id
        )
        items.append(status)
        if status["state"] != "remote":
            seen_sessions.add(status["root_folder_id"])
    # A workspace can also reach a shared local copy through a recursive
    # ancestor: e.g. this workspace links /parent while another workspace
    # stages /parent/child. Staging rebased the child's folders.path under
    # local-folders/, so the loop above (keyed on the user-facing root)
    # sees folder_status(/parent) as remote and would report the workspace
    # as fully remote — hiding local work and offering Work Locally instead
    # of sync/discard. Surface each descendant session as its own item so
    # the UI reflects that this workspace's catalog is partly rebased.
    for local_root_id in workspace_local_root_ids(db, workspace_id):
        if local_root_id in seen_sessions:
            continue
        descendant_status = folder_status(db, local_root_id, vireo_dir)
        descendant_status["requested_folder_id"] = local_root_id
        descendant_status["display_path"] = descendant_status.get("source_path") or ""
        descendant_status["folder_name"] = _folder_name_for(
            db, local_root_id, descendant_status["display_path"]
        )
        visible_ancestor_id = _nearest_visible_ancestor(
            db, local_root_id, workspace_id
        )
        descendant_status["visible_ancestor_folder_id"] = visible_ancestor_id
        # Normally the user-facing ancestor owns the subtree tally, so
        # repeating it on this session would double-count. If that ancestor
        # is missing, however, Browse has to synthesize this descendant as
        # the only visible recovery row. Give that row the session's exact
        # workspace-scoped count instead of displaying a misleading zero.
        descendant_status["workspace_photo_count"] = (
            0
            if visible_ancestor_id is not None
            else _workspace_local_session_photo_count(
                db, workspace_id, local_root_id
            )
        )
        items.append(descendant_status)
        if descendant_status["state"] != "remote":
            seen_sessions.add(descendant_status["root_folder_id"])
    local_count = sum(1 for item in items if item["state"] != "remote")
    state = "remote" if local_count == 0 else "active" if local_count == len(items) else "mixed"
    linked_workspace_ids = sorted({
        workspace_id
        for item in items
        for workspace_id in item.get("workspace_ids", [])
    })
    return {
        "state": state,
        "workspace_id": int(workspace_id),
        "folder_count": len(items),
        "local_folder_count": local_count,
        "session_count": len(seen_sessions),
        "folders": items,
        "linked_workspaces": workspace_summaries_for_ids(db, linked_workspace_ids),
    }


def _derive_folder_name(path: str) -> str:
    if not path:
        return ""
    normalized = path.replace("\\", "/").rstrip("/")
    return normalized.rsplit("/", 1)[-1] or normalized


def _folder_name_for(db, folder_id: int, fallback_path: str) -> str:
    row = db.get_folder(int(folder_id))
    if row and row["name"]:
        return row["name"]
    return _derive_folder_name(fallback_path)


def _nearest_visible_ancestor(db, folder_id: int, workspace_id: int) -> int | None:
    """Return the nearest ancestor of ``folder_id`` that is linked to
    ``workspace_id`` and would appear in ``get_folder_tree`` (status ok or
    partial). Returns ``None`` if no such ancestor exists.

    The Browse folder tree drops rows whose status is ``missing`` (see
    ``get_folder_tree``). A local session whose managed directory is deleted
    or unmounted has its rebased ``folders.path`` marked missing by
    ``check_folder_health``, so the requested folder ID vanishes from the
    rendered tree. Callers can attach the LOCAL ISSUE recovery badge to this
    ancestor instead so users still see the affected subtree flagged.
    """
    row = db.workspace_folders.nearest_visible_ancestor(folder_id, workspace_id)
    return int(row["id"]) if row else None


def _restore_catalog(db, root_folder_id: int) -> None:
    mappings = _mappings(db, root_folder_id)
    mapped_ids = {item["folder_id"] for item in mappings}
    root = next((item for item in mappings if item["is_root"]), None)
    if root is None:
        raise LocalWorkspaceError("Local folder mapping is missing its root")
    db.begin_immediate()
    try:
        for mapping in mappings:
            conflict = db.local_folders.path_conflict(mapping["source_path"], mapping["folder_id"])
            if conflict and conflict["id"] not in mapped_ids:
                db._merge_into_existing(
                    conflict["id"], mapping["folder_id"], mapping["source_path"], commit=False
                )
        for mapping in mappings:
            db.local_folders.set_catalog_path(f"__vireo_local_folder_restore__/{root_folder_id}/{mapping['folder_id']}", mapping["folder_id"])
        for mapping in mappings:
            db.local_folders.restore_catalog_folder(mapping)

        relinked = list(mapped_ids)
        for row in db.local_folders.catalog_rows():
            if row["id"] in mapped_ids or not _is_within(row["path"], root["local_path"]):
                continue
            target = os.path.normpath(
                os.path.join(root["source_path"], _relative(row["path"], root["local_path"]))
            )
            existing = db.local_folders.path_conflict(target, row["id"])
            if existing:
                db._merge_into_existing(row["id"], existing["id"], target, commit=False)
            else:
                db.local_folders.set_catalog_path(target, row["id"])
                relinked.append(row["id"])
        db._relink_parents_by_path(relinked)
        _delete_state_rows(db, root_folder_id)
        db.commit()
    except BaseException:
        db.rollback()
        raise


def sync_folder(
    db,
    root_folder_id: int,
    vireo_dir: str,
    *,
    allow_deletions: bool = False,
    confirmed_deletions: int | None = None,
    progress=None,
    scan_progress=None,
    cancel_check=None,
    begin_commit=None,
) -> dict:
    """Publish one shared local folder and restore its catalog paths.

    ``scan_progress(current, total, rel)`` reports the conflict scan that runs
    before anything is published, and ``progress(current, total, rel)`` the
    publish itself. The scan re-reads every at-risk source file over the
    network, so on a large folder it owns most of the job's wall clock; it is
    reported separately rather than left as a silent wait. ``current`` counts
    entries already compared and ``rel`` names the one being compared now.
    """
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
        added = [key for key in changed if key not in baseline]
        # Every at-risk source entry is re-read (and usually re-hashed) before
        # a single byte is published, so this loop is the slow half of a sync.
        # Count the files up front and report each one as it is checked.
        scan_total = len(at_risk) + len(added)
        scanned = 0

        def note_scanning(rel):
            """Name the entry about to be compared, before it is counted.

            The count is what the job's throughput and ETA are derived from,
            so it may only advance once an entry has actually been read —
            hashing one multi-gigabyte original over SMB is minutes of work
            that a count-on-entry would already be showing as finished.
            """
            if scan_progress:
                scan_progress(scanned, scan_total, rel)

        for key in at_risk:
            index, rel = key
            note_scanning(rel)
            try:
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
            finally:
                scanned += 1

        deleted_set = set(deleted)
        for key in added:
            index, rel = key
            note_scanning(rel)
            try:
                remote_path = os.path.join(manifest["roots"][index]["source_path"], rel)
                if not os.path.lexists(remote_path):
                    continue
                if os.path.isdir(remote_path) and not os.path.islink(remote_path):
                    conflicts.extend(
                        full
                        for entry_rel, full, st in _walk_entries(remote_path)
                        if _entry_type(st) != "dir"
                        and (index, os.path.join(rel, entry_rel)) not in deleted_set
                    )
                elif not _matches_remote(local[key][0], remote_path, None, cancel_check):
                    conflicts.append(remote_path)
            finally:
                scanned += 1

        if scan_progress:
            scan_progress(scanned, scan_total, "")
        conflicts.extend(_ancestor_conflicts(list(changed) + list(deleted), manifest, deleted_set))
        if conflicts:
            raise LocalWorkspaceConflict(sorted(set(conflicts)))

        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("Local folder sync cancelled")
        if begin_commit and not begin_commit():
            raise LocalWorkspaceCancelled("Local folder sync cancelled")
        if not resuming:
            _write_sync_recovery(vireo_dir, root_folder_id, deleted)
            db.local_folders.mark_syncing(root_folder_id)
            db.commit()
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
        _remove_folder_dir(vireo_dir, root_folder_id, root["local_path"])
        _invalidate_new_images_for_source(
            db, root["source_path"], workspace_ids,
        )
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
            root = _root_mapping(db, root_folder_id)
            _remove_folder_dir(
                vireo_dir, root_folder_id, root["local_path"] if root else None
            )
            _delete_state_rows(db, root_folder_id)
            db.commit()
            _invalidate_new_images_for_source(
                db, root["source_path"] if root else None,
            )
            return {"ok": True, "root_folder_id": root_folder_id, "discarded": True}
        if state == "syncing" and not acknowledge_published:
            raise LocalWorkspaceError(
                "A sync-back was interrupted after some files were published. Finish syncing, or acknowledge that unpublished changes will be lost."
            )
        if state not in {"active", "syncing"}:
            raise LocalWorkspaceError("Local folder is not in a recoverable state")
        root = _root_mapping(db, root_folder_id)
        if root is None:
            raise LocalWorkspaceError("Local folder mapping is missing its root")
        workspace_ids = affected_workspace_ids(db, root_folder_id)
        _restore_catalog(db, root_folder_id)
        _remove_folder_dir(vireo_dir, root_folder_id, root["local_path"])
        _invalidate_new_images_for_source(
            db, root["source_path"], workspace_ids,
        )
        return {"ok": True, "root_folder_id": root_folder_id, "discarded": True}
