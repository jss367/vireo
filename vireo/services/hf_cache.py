"""The Hugging Face hub cache entries Vireo downloaded, and their cleanup.

Every model Vireo fetches goes through ``hf_hub_download``, which stores the
bytes in the hub cache (``blobs/<oid>`` plus ``snapshots/<rev>/<path>``
symlinks, or plain copies where symlinks are unavailable) before
``models.py`` clones them into ``~/.vireo/models``. The runtime only ever
reads ``~/.vireo/models``, so a cache entry is useful exactly while the model
it came from is installed: it lets Repair or a re-download skip the network.
Once the model is removed, or a newer snapshot replaces it, the entry is dead
weight that Settings never shows (Settings reads only the install folder).

``inventory`` lists the cache per model with that distinction, ``remove``
deletes chosen entries, and ``purge_model`` / ``purge_subfolder`` are the
hooks the Settings deletes call so the cache copy goes with the install.

Keys are ``<repo>/<subfolder>`` for the shared ONNX repo (one per model
subfolder across every snapshot), ``<repo>`` for any other repo (or the ONNX
repo's top-level files), and ``<repo>#unreferenced`` for blobs no snapshot
points at any more.

Sizes are reclaimable bytes: a blob two entries share (an identical
``config.json``) is counted in neither entry's ``size`` and in
``unused_size`` only when no installed entry also references it, because
deletion keeps any blob a remaining snapshot still links.

An entry is ``installed`` when a model directory exists for it, even an
empty one: every download creates the directory before fetching, so the
directory is the user's intent and the cache entry is what will fill it.

Cleanup scope is the explicit list from ``KNOWN_MODELS`` and the
managed registered models (plus the legacy BioCLIP checkpoints), never an
organization-wide prefix: a shared cache may hold repositories owned by
other Hub clients, so registration alone cannot authorize cleanup. A purge of a dynamically downloaded model
(``download_hf_model``, id absent from ``KNOWN_MODELS``) passes the entry's
``hf-hub:`` identifier captured before unregistration so that one repository
rejoins the collection scope for the purge. Read-only inventory also reports
unmanaged registered repositories, with their payloads marked non-removable.

Cleanup is descriptor-only and fails closed on unsupported platforms. A pinned
cache tree spans collection, selection and payload deletion. All Vireo cache
producers share a cross-process lock; the selected blobs' hub lock targets are
created/acquired even when absent at enumeration. A later download to a different
blob is unselected and untouched. Directory shells and refs/marker metadata are
never pruned: newly arriving or unselected contents cannot be swept up. Plain
snapshot copies without a blob link are retained because their download lock
cannot be established from the cache entry alone.

"""

from __future__ import annotations

import logging
import os
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING

from services.hf_cache_lock import DOWNLOAD_LOCK
from services.hf_cache_tree import CacheTree, HubLocks, UnsafeCacheCleanup

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Iterable, Iterator, Mapping, Sequence
    from typing import NoReturn, Protocol, TypedDict

    from services.service_types import JsonDict

    class _PathOps(Protocol):
        """The ``os.path`` predicates the collection walk uses."""

        def isdir(self, path: str, /) -> bool: ...
        def isfile(self, path: str, /) -> bool: ...
        def islink(self, path: str, /) -> bool: ...
        def realpath(self, path: str, /) -> str: ...

    class _CacheFs(Protocol):
        """``os`` itself, or the ``CacheTree`` pinned for a cleanup."""

        @property
        def path(self) -> _PathOps: ...
        def listdir(self, path: str, /) -> list[str]: ...
        def lstat(self, path: str, /) -> os.stat_result: ...
        def walk(self, path: str, /) -> Iterator[tuple[str, list[str], list[str]]]: ...

    # The TypedDicts live here because the descriptor-safety tests execute
    # this module's top-level classes and functions without its imports.
    class _SnapshotFiles(TypedDict):
        """One group's snapshot files and the blob (with size) each resolves to."""

        files: list[str]
        blobs: dict[str, int]

    class CacheEntry(TypedDict):
        """One ``inventory`` entry, as the Storage page shows it."""

        key: str
        name: str
        size: int
        shared_size: int
        installed: bool
        install_path: str | None
        file_count: int
        retained_size: int
        removable: bool
        retained_reason: str | None

    class CacheInventory(TypedDict):
        """What ``inventory`` returns."""

        path: str
        size: int
        entries: list[CacheEntry]
        unused_size: int
        retained_size: int
        installed_size: int

    class CacheRemovalFailure(TypedDict):
        """A snapshot link or blob a removal could not delete."""

        key: str
        path: str
        error: str

    class CacheRemoval(TypedDict):
        """What ``remove`` / ``remove_unused`` return (and a failed purge carries)."""

        removed: list[str]
        failed: list[CacheRemovalFailure]
        freed: int


log = logging.getLogger(__name__)

UNREFERENCED_SUFFIX = "#unreferenced"
_ACTIVE_TREE: ContextVar[CacheTree | None] = ContextVar("hf_cache_tree", default=None)


def _fs() -> _CacheFs:
    tree = _ACTIVE_TREE.get()
    return tree if tree is not None else os


class DownloadInProgress(RuntimeError):
    """Cleanup refused because another model or cache operation may be active."""


class CachePurgeIncomplete(RuntimeError):
    """Model removal finished, but its cache purge did not fully succeed."""

    def __init__(self, result: CacheRemoval) -> None:
        super().__init__("Some Hugging Face cache entries could not be removed")
        self.result = result

# A partial download HF can still resume. Older than this it is abandoned.
INCOMPLETE_GRACE_SECS = 24 * 3600


def _models_dir(models_dir: str | None) -> str:
    if models_dir is not None:
        return models_dir
    import models
    return models.DEFAULT_MODELS_DIR


def _cache_dir(cache_dir: str | None) -> str:
    if cache_dir is not None:
        return cache_dir
    from services.storage_paths import huggingface_cache_dir
    return huggingface_cache_dir()


def _repo_dirs(
    cache_dir: str, extra_repos: Iterable[str] = (), *, managed_only: bool = True,
) -> list[tuple[str, str]]:
    """Vireo-relevant ``models--*`` directories under the hub cache (no symlinks).

    The whitelist is the explicit list built from ``KNOWN_MODELS`` and the
    managed registered models (plus the legacy BioCLIP checkpoints), never an
    organization-wide prefix: a shared cache may hold repositories owned by
    other Hugging Face clients, and inventorying one of those as a Vireo
    entry would let ``Remove unused`` evict it. ``extra_repos`` names repo
    ids (``owner/name``) a specific purge wants collected on top of the
    whitelist — used when the entry being purged has already been
    unregistered, so its repo is no longer in ``huggingface_repositories()``.
    """
    from services.storage_paths import huggingface_repositories

    if not _fs().path.isdir(cache_dir):
        return []
    wanted = huggingface_repositories(managed_only=managed_only) | {
        "models--" + repo.replace("/", "--") for repo in extra_repos
    }
    out = []
    for name in sorted(_fs().listdir(cache_dir)):
        if name in wanted:
            path = os.path.join(cache_dir, name)
            if _fs().path.isdir(path) and not _fs().path.islink(path):
                out.append((name, path))
    return out


def _repo_id(dir_name: str) -> str:
    return dir_name.removeprefix("models--").replace("--", "/")


def _onnx_repo_dir_name() -> str:
    from models import ONNX_REPO
    return "models--" + ONNX_REPO.replace("/", "--")


def _is_install_dir(path: str) -> bool:
    """A model directory, even an empty one a download has just created."""
    return bool(path) and os.path.isdir(path)


def _within(path: str, root: str) -> bool:
    """True when ``path``'s parent resolves to ``root`` or somewhere beneath it."""
    root = _fs().path.realpath(root)
    parent = _fs().path.realpath(os.path.dirname(path))
    return parent == root or parent.startswith(root + os.sep)


def _install_candidates(models_dir: str) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Map cache groups to the directories that would hold their install.

    Returns ``{onnx_subfolder: [dirs]}`` and ``{repo_id: [dirs]}``.
    """
    import models as models_mod

    by_subfolder: dict[str, list[str]] = {}
    by_repo: dict[str, list[str]] = {}
    registered = {}
    try:
        registered = {m["id"]: m for m in models_mod.get_models()}
    except Exception as exc:  # pragma: no cover - models.json corruption
        log.warning("Could not read registered models for HF cache inventory: %s", exc)
    for km in models_mod.KNOWN_MODELS:
        dirs = [os.path.join(models_dir, km["id"])]
        reg = registered.get(km["id"]) or {}
        if reg.get("weights_path"):
            dirs.append(reg["weights_path"])
        by_subfolder.setdefault(km.get("hf_subdir", km["id"]), []).extend(dirs)
        for field in ("model_str", "source"):
            value = km.get(field, "")
            if value.startswith("hf-hub:"):
                by_repo.setdefault(value.removeprefix("hf-hub:"), []).extend(dirs)
    for entry in registered.values():
        value = entry.get("model_str", "")
        if value.startswith("hf-hub:") and entry.get("weights_path"):
            by_repo.setdefault(value.removeprefix("hf-hub:"), []).append(entry["weights_path"])
    return by_subfolder, by_repo


def _display_name(subfolder: str) -> str:
    import models as models_mod
    for km in models_mod.KNOWN_MODELS:
        if km.get("hf_subdir", km["id"]) == subfolder:
            return f"{km['name']} ({subfolder})"
    return subfolder


def _record(entry: _SnapshotFiles, path: str) -> str | None:
    """Add one snapshot file to a group, resolving a symlink to its blob."""
    entry["files"].append(path)
    target = _fs().path.realpath(path) if _fs().path.islink(path) else path
    try:
        entry["blobs"][target] = _fs().lstat(target).st_size
    except OSError:
        entry["blobs"][target] = 0
    return target if target != path else None


def _scan_repo(repo_dir: str, onnx: bool) -> tuple[dict[str, _SnapshotFiles], set[str]]:
    """Return ``(groups, referenced_blobs)`` for one repo directory.

    ``groups`` maps a group name (the ONNX subfolder, or ``""`` for a whole
    repo and for the ONNX repo's top-level files) to
    ``{"files": [snapshot paths], "blobs": {path: size}}``. Plain files
    (no-symlink layouts) are recorded as their own blob. Symlinked
    directories are not entered at any level.
    """
    groups: dict[str, _SnapshotFiles] = {}
    referenced: set[str] = set()
    snapshots = os.path.join(repo_dir, "snapshots")
    if _fs().path.islink(snapshots) or not _fs().path.isdir(snapshots):
        return groups, referenced
    for rev in sorted(_fs().listdir(snapshots)):
        rev_dir = os.path.join(snapshots, rev)
        if _fs().path.islink(rev_dir) or not _fs().path.isdir(rev_dir):
            continue
        for root, dirs, files in _fs().walk(rev_dir):
            dirs[:] = [d for d in dirs if not _fs().path.islink(os.path.join(root, d))]
            for name in files:
                path = os.path.join(root, name)
                rel = os.path.relpath(path, rev_dir)
                group = rel.split(os.sep, 1)[0] if onnx and os.sep in rel else ""
                entry = groups.setdefault(group, {"files": [], "blobs": {}})
                blob = _record(entry, path)
                if blob:
                    referenced.add(blob)
    return groups, referenced


def _blob_files(repo_dir: str) -> dict[str, os.stat_result]:
    """``{name: lstat}`` for the regular files directly in ``blobs/``."""
    blobs_dir = os.path.join(repo_dir, "blobs")
    if _fs().path.islink(blobs_dir) or not _fs().path.isdir(blobs_dir):
        return {}
    out = {}
    for name in _fs().listdir(blobs_dir):
        path = os.path.join(blobs_dir, name)
        try:
            st = _fs().lstat(path)
        except OSError:
            continue
        if _fs().path.isfile(path) and not _fs().path.islink(path):
            out[path] = st
    return out


def _unreferenced_blobs(repo_dir: str, referenced: Container[str]) -> dict[str, int]:
    cutoff = time.time() - INCOMPLETE_GRACE_SECS
    out = {}
    for path, st in _blob_files(repo_dir).items():
        if _fs().path.realpath(path) in referenced:
            continue
        if path.endswith(".incomplete") and st.st_mtime > cutoff:
            continue
        out[_fs().path.realpath(path)] = st.st_size
    return out


def _reclaimable_blobs(group: JsonDict) -> dict[str, int]:
    """Only eligible hub blobs have download locks cleanup can establish."""
    if not group.get("cleanup_eligible", True):
        return {}
    blobs_dir = _fs().path.realpath(os.path.join(group["repo_dir"], "blobs"))
    return {path: size for path, size in group["blobs"].items()
            if os.path.dirname(path) == blobs_dir}


def _size_groups(found: Mapping[str, JsonDict]) -> None:
    """Set each group's ``size`` to the bytes only it references."""
    referrers: dict[str, set[str]] = {}
    for group in found.values():
        for blob in _reclaimable_blobs(group):
            referrers.setdefault(blob, set()).add(group["key"])
    for group in found.values():
        reclaimable = _reclaimable_blobs(group)
        exclusive = [s for b, s in reclaimable.items() if referrers[b] == {group["key"]}]
        group["size"] = sum(exclusive)
        group["shared_size"] = sum(reclaimable.values()) - group["size"]
        group["retained_size"] = sum(s for b, s in group["blobs"].items() if b not in reclaimable)
        group["removable"] = bool(reclaimable)


def _collect(
    cache_dir: str, models_dir: str, extra_repos: Sequence[str] = (), *, reporting: bool = False,
) -> tuple[int, dict[str, JsonDict]]:
    """Every group across every Vireo repo, keyed, with install status."""
    from services.storage_paths import directory_bytes

    by_subfolder, by_repo = _install_candidates(models_dir)
    onnx_name = _onnx_repo_dir_name()
    total = 0
    found = {}
    eligible = {name for name, _path in _repo_dirs(cache_dir, extra_repos)} if reporting else None
    for dir_name, repo_dir in _repo_dirs(cache_dir, extra_repos, managed_only=not reporting):
        cleanup_eligible = eligible is None or dir_name in eligible
        total += directory_bytes(repo_dir) if _ACTIVE_TREE.get() is None else 0
        repo = _repo_id(dir_name)
        onnx = dir_name == onnx_name
        groups, referenced = _scan_repo(repo_dir, onnx)
        for group, data in groups.items():
            if onnx and group:
                key, name = f"{repo}/{group}", _display_name(group)
                candidates = by_subfolder.get(group, []) + [os.path.join(models_dir, group)]
            elif onnx:
                key, name, candidates = repo, f"{repo}: repository metadata", []
            else:
                key, name, candidates = repo, repo, by_repo.get(repo, [])
            install = next((d for d in candidates if _is_install_dir(d)), None)
            found[key] = {
                "key": key, "name": name, "repo_dir": repo_dir, "cleanup_eligible": cleanup_eligible,
                "files": data["files"], "blobs": data["blobs"],
                "installed": install is not None, "install_path": install,
            }
        dangling = _unreferenced_blobs(repo_dir, referenced)
        if dangling:
            key = repo + UNREFERENCED_SUFFIX
            found[key] = {
                "key": key, "name": f"{repo}: files no download refers to",
                "repo_dir": repo_dir, "files": list(dangling), "blobs": dangling, "cleanup_eligible": cleanup_eligible,
                "installed": False, "install_path": None, "dangling": True,
            }
    _size_groups(found)
    return total, found


def _public(group: JsonDict) -> CacheEntry:
    return {
        "key": group["key"], "name": group["name"], "size": group["size"],
        "shared_size": group["shared_size"], "installed": group["installed"],
        "install_path": group["install_path"], "file_count": len(group["files"]),
        "retained_size": group["retained_size"], "removable": group["removable"],
        "retained_reason": ("Reported for an unmanaged registration; Vireo does not own this repository's cache cleanup."
                            if not group.get("cleanup_eligible", True) else None),
    }


def inventory(cache_dir: str | None = None, models_dir: str | None = None) -> CacheInventory:
    """Describe the Vireo entries in the hub cache, per model.

    ``size`` is the bytes the Vireo repos occupy on disk; ``entries`` splits
    them by model with ``installed`` saying whether an install folder exists
    for it. ``unused_size`` is what ``remove_unused`` would reclaim: every
    eligible blob the unused entries reference that no installed entry also
    does. Unmanaged registrations remain visible with non-removable retained
    bytes; installed bytes take priority over retained bytes in the totals.
    """
    cache_dir = _cache_dir(cache_dir)
    total, found = _collect(cache_dir, _models_dir(models_dir), reporting=True)
    installed_blobs: dict[str, int] = {}
    unused_blobs: dict[str, int] = {}
    retained_blobs: dict[str, int] = {}
    for group in found.values():
        if group["installed"]:
            installed_blobs.update(group["blobs"])
        else:
            reclaimable = _reclaimable_blobs(group)
            unused_blobs.update(reclaimable)
            retained_blobs.update({b: s for b, s in group["blobs"].items() if b not in reclaimable})
    ordered = sorted(found.values(), key=lambda g: (-g["size"], g["key"]))
    return {
        "path": cache_dir,
        "size": total,
        "entries": [_public(g) for g in ordered],
        "unused_size": sum(s for b, s in unused_blobs.items() if b not in installed_blobs),
        "retained_size": sum(retained_blobs.values()),
        "installed_size": sum(installed_blobs.values()),
    }


def _remove_repo_if_empty(_repo_dir: str) -> bool:
    """Keep directory anchors and metadata, including after the last payload.

    Even empty-directory pruning can race a rename replacement. No cleanup
    path removes directory bindings, refs, markers, or newly arriving files.
    """
    return False


def _unlink_within(root: str, path: str) -> None:
    tree = _ACTIVE_TREE.get()
    if tree is None or os.path.abspath(root) != tree.root:
        raise UnsafeCacheCleanup("Deletion requires the tree pinned during collection")
    tree.unlink(path)


def _rmdir_within(_root: str, _path: str) -> NoReturn:
    raise UnsafeCacheCleanup("Cache directory pruning is disabled")


def _download_lock_targets(found: Mapping[str, JsonDict]) -> set[str]:
    """Lock both existing hub locks and all blob etags, even absent lockfiles."""
    targets = set()
    for repo_dir in dict.fromkeys(group["repo_dir"] for group in found.values()):
        lock_dir = os.path.join(os.path.dirname(repo_dir), ".locks", os.path.basename(repo_dir))
        if _fs().path.isdir(lock_dir):
            targets.update(os.path.join(lock_dir, name) for name in _fs().listdir(lock_dir)
                           if name.endswith(".lock"))
        blobs_dir = os.path.join(repo_dir, "blobs")
        blobs = set(_blob_files(repo_dir))
        for group in found.values():
            if group["repo_dir"] == repo_dir:
                # Broken snapshot links still name future download targets.
                blobs.update(blob for blob in group["blobs"]
                             if os.path.dirname(blob) == blobs_dir)
        for blob in blobs:
            etag = os.path.basename(blob).removesuffix(".incomplete")
            targets.add(os.path.join(lock_dir, etag + ".lock"))
    return targets


def _unlink_snapshot_files(group: JsonDict, failures: list[CacheRemovalFailure]) -> int:
    """Remove selected snapshot links; retain unverifiable plain copies."""
    cache_dir = os.path.dirname(group["repo_dir"])
    snapshots = os.path.join(group["repo_dir"], "snapshots")
    for path in group["files"]:
        if not _within(path, snapshots) or not _fs().path.islink(path):
            failures.append({"key": group["key"], "path": path,
                             "error": "snapshot is outside the repository or has no verifiable blob lock"})
            continue
        try:
            _unlink_within(cache_dir, path)
        except OSError as exc:
            log.warning("Could not remove HF cache entry %s: %s", path, exc)
            failures.append({"key": group["key"], "path": path, "error": str(exc)})
            continue
    return 0  # Reclaimable payload bytes are counted at their blob, once.


def _delete_groups(groups: Iterable[JsonDict]) -> tuple[int, list[CacheRemovalFailure]]:
    """Private callers cannot supply stale paths outside locked collection."""
    if _ACTIVE_TREE.get() is None:
        raise UnsafeCacheCleanup("Groups must be collected inside the cleanup transaction")
    return _delete_groups_locked(groups)


def _collect_locked(
    cache_dir: str, models_dir: str, locks: HubLocks, extra_repos: Sequence[str] = (),
) -> dict[str, JsonDict]:
    # Recollect after acquiring newly discovered targets. A late *different*
    # blob is never selected without its lock; same-etag writers must wait.
    for _attempt in range(4):
        _total, found = _collect(cache_dir, models_dir, extra_repos)
        targets = _download_lock_targets(found)
        if targets <= locks.held.keys():
            return found
        try:
            locks.hold(targets)
        except BlockingIOError:
            raise DownloadInProgress("another process holds a hub download lock") from None
    raise DownloadInProgress("cache download targets keep changing")


def _delete_selected(
    cache_dir: str, models_dir: str, select: Callable[[dict[str, JsonDict]], list[JsonDict]], *,
    extra_repos: Sequence[str] = (),
) -> CacheRemoval:
    """Pin first, then collect/select/delete under the same download guards."""
    try:
        acquired = DOWNLOAD_LOCK.acquire(blocking=False)
    except (ImportError, OSError) as exc:
        raise UnsafeCacheCleanup(f"Cannot establish the shared model/cache guard: {exc}") from exc
    if not acquired:
        raise DownloadInProgress("another model or cache operation is in progress")
    try:
        # Resolve the cache root once, matching ``CacheTree.__init__``, so
        # a user-owned symlink in an ancestor (``~/.cache -> /mnt/data``,
        # ``/tmp -> /private/tmp``) is followed before any ``O_NOFOLLOW``
        # descent, and every path collected and verified below is built
        # from the same canonical root. ``_relative`` compares against
        # ``tree.root`` and would otherwise reject paths derived from an
        # unresolved cache_dir. ``realpath`` of a missing path returns it
        # unchanged, so the lexists guard below still rejects a cache dir
        # that never existed.
        cache_dir = _fs().path.realpath(cache_dir)
        if not os.path.lexists(cache_dir):
            return _result(select({}), 0, [])
        try:
            with CacheTree(cache_dir) as tree, HubLocks(tree) as locks:
                token = _ACTIVE_TREE.set(tree)
                try:
                    found = _collect_locked(cache_dir, models_dir, locks, extra_repos)
                    groups = select(found)
                    freed, failures = _delete_groups_locked(groups)
                    return _result(groups, freed, failures)
                finally:
                    _ACTIVE_TREE.reset(token)
        except OSError as exc:
            raise UnsafeCacheCleanup(f"Cache changed or cannot be safely accessed: {exc}") from exc

    finally:
        DOWNLOAD_LOCK.release()


def _delete_groups_locked(groups: Iterable[JsonDict]) -> tuple[int, list[CacheRemovalFailure]]:
    freed = 0
    failures: list[CacheRemovalFailure] = []
    # repo_dir -> blob -> (owner keys, size); a blob two selected groups share
    # has every owner recorded so a failed unlink is attributed to each.
    candidate_blobs: dict[str, dict[str, tuple[set[str], int]]] = {}
    for group in groups:
        repo_dir = group["repo_dir"]
        if not group.get("dangling"):
            freed += _unlink_snapshot_files(group, failures)
        blobs_dir = os.path.join(repo_dir, "blobs")
        for blob, size in group["blobs"].items():
            if _within(blob, blobs_dir) and not _fs().path.islink(blob):
                owners, _size = candidate_blobs.setdefault(repo_dir, {}).setdefault(
                    blob, (set(), size))
                owners.add(group["key"])
    for repo_dir, blobs in candidate_blobs.items():
        _groups, still_referenced = _scan_repo(repo_dir, onnx=True)
        cache_root = _fs().path.realpath(os.path.dirname(repo_dir))  # blobs are realpaths
        for blob, (owners, size) in blobs.items():
            if blob in still_referenced or not _fs().path.isfile(blob):
                continue
            try:
                _unlink_within(cache_root, blob)
                freed += size
            except OSError as exc:
                log.warning("Could not remove HF cache blob %s: %s", blob, exc)
                failures.extend(
                    {"key": key, "path": blob, "error": str(exc)} for key in sorted(owners))
    return freed, failures


def _result(groups: Iterable[JsonDict], freed: int, failures: list[CacheRemovalFailure]) -> CacheRemoval:
    failed_keys = {f["key"] for f in failures}
    return {
        "removed": [g["key"] for g in groups if g["key"] not in failed_keys],
        "failed": failures,
        "freed": freed,
    }


def remove(keys: Sequence[str], cache_dir: str | None = None, models_dir: str | None = None) -> CacheRemoval:
    """Remove the entries named by ``keys``.

    Unknown keys raise ``KeyError``; a key whose entry backs an installed
    model raises ``ValueError``. Both are decided on state collected under
    the download locks, not on what the page inventoried earlier.
    """
    def select(found: dict[str, JsonDict]) -> list[JsonDict]:
        missing = [k for k in keys if k not in found]
        if missing:
            raise KeyError(missing[0])
        groups = [found[k] for k in dict.fromkeys(keys)]
        installed = [g["key"] for g in groups if g["installed"]]
        if installed:
            raise ValueError(installed[0])
        return groups

    result = _delete_selected(_cache_dir(cache_dir), _models_dir(models_dir), select)
    log.info("Removed HF cache entries %s (%d bytes, %d failures)",
             list(keys), result["freed"], len(result["failed"]))
    return result


def remove_unused(cache_dir: str | None = None, models_dir: str | None = None) -> CacheRemoval:
    """Remove every entry that backs no installed model."""
    result = _delete_selected(
        _cache_dir(cache_dir), _models_dir(models_dir),
        lambda found: [g for g in found.values() if not g["installed"] and g["removable"]],
    )
    log.info("Removed %d unused HF cache entries (%d bytes, %d failures)",
             len(result["removed"]) + len(result["failed"]), result["freed"], len(result["failed"]))
    return result


def _purge_keys(
    keys: Sequence[str], cache_dir: str, models_dir: str, *, extra_repos: Sequence[str] = (),
) -> int:
    """Delete the entries for ``keys`` that exist and back no model directory.

    Collected under the download locks like a Storage-page removal: the
    Settings delete that calls this has just removed the install directory,
    so an entry that is installed again means a reinstall began in between,
    and its cache copy is exactly what that reinstall needs.
    ``extra_repos`` widens the collection scope for keys whose repository is
    not in the ``huggingface_repositories()`` whitelist (dynamically
    downloaded models whose entry has already been unregistered).
    """
    result = _delete_selected(
        cache_dir, models_dir,
        lambda found: [found[k] for k in dict.fromkeys(keys)
                       if k in found and not found[k]["installed"]],
        extra_repos=extra_repos,
    )
    if result["failed"]:
        raise CachePurgeIncomplete(result)
    return result["freed"]


def purge_outcome(operation: Callable[[str], object], identifier: str) -> JsonDict:
    """Preserve model-removal success and report any cache cleanup failure."""
    result: Mapping[str, object]
    try:
        operation(identifier)
        return {}
    except CachePurgeIncomplete as exc:
        result = exc.result
    except Exception as exc:  # Report the failure to the caller below, with a warning.
        result = {"freed": 0, "failed": [{"key": identifier, "error": str(exc)}]}
    log.warning("Model removed but HF cache cleanup incomplete for %s: %s", identifier, result)
    return {"ok": False, "cache_cleanup": result}


def purge_subfolder(subfolder: str, cache_dir: str | None = None, models_dir: str | None = None) -> int:
    """Drop the ONNX repo's cache entry for ``subfolder`` after its install went.

    Called after a Settings delete, so the cache copy leaves with the install
    instead of lingering invisibly. Returns the bytes freed.
    """
    from models import ONNX_REPO

    freed = _purge_keys([f"{ONNX_REPO}/{subfolder}"], _cache_dir(cache_dir), _models_dir(models_dir))
    log.info("Purged HF cache entry for %s (%d bytes)", subfolder, freed)
    return freed


def purge_model(
    model_id: str, cache_dir: str | None = None, models_dir: str | None = None, *, hf_source: str = "",
) -> int:
    """Drop every cache entry a removed model's download produced.

    ``hf_source`` is the ``hf-hub:<owner>/<repo>`` identifier captured from
    the entry before unregistration. It is required for a dynamically
    downloaded model (``download_hf_model``) whose id is absent from
    ``KNOWN_MODELS`` and whose repo has already left the
    ``huggingface_repositories()`` whitelist; without it the purge would
    return ``0`` and leave the cache payload behind. For a ``KNOWN_MODELS``
    entry it agrees with what the manifest already names and is a harmless
    hint.
    """
    import models as models_mod
    from models import ONNX_REPO

    keys = []
    extra_repos = []
    km = next((m for m in models_mod.KNOWN_MODELS if m["id"] == model_id), None)
    if km is not None:
        keys.append(f"{ONNX_REPO}/{km.get('hf_subdir', model_id)}")
        for field in ("model_str", "source"):
            value = km.get(field, "")
            if value.startswith("hf-hub:"):
                keys.append(value.removeprefix("hf-hub:"))
    if hf_source and hf_source.startswith("hf-hub:"):
        repo = hf_source.removeprefix("hf-hub:")
        if repo not in keys:
            keys.append(repo)
        extra_repos.append(repo)
    if not keys:
        return 0
    return _purge_keys(keys, _cache_dir(cache_dir), _models_dir(models_dir),
                       extra_repos=extra_repos)
