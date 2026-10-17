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

from services.hf_cache_lock import DOWNLOAD_LOCK
from services.hf_cache_tree import CacheTree, HubLocks, UnsafeCacheCleanup

log = logging.getLogger(__name__)

UNREFERENCED_SUFFIX = "#unreferenced"
_ACTIVE_TREE: ContextVar[CacheTree | None] = ContextVar("hf_cache_tree", default=None)


def _fs():
    tree = _ACTIVE_TREE.get()
    return tree if tree is not None else os


class DownloadInProgress(RuntimeError):
    """Cleanup refused because a model download may be writing the cache."""


class CachePurgeIncomplete(RuntimeError):
    """Model removal finished, but its cache purge did not fully succeed."""

    def __init__(self, result):
        super().__init__("Some Hugging Face cache entries could not be removed")
        self.result = result

# A partial download HF can still resume. Older than this it is abandoned.
INCOMPLETE_GRACE_SECS = 24 * 3600


def _models_dir(models_dir):
    if models_dir is not None:
        return models_dir
    import models
    return models.DEFAULT_MODELS_DIR


def _cache_dir(cache_dir):
    if cache_dir is not None:
        return cache_dir
    from services.storage_paths import huggingface_cache_dir
    return huggingface_cache_dir()


def _repo_dirs(cache_dir):
    """Vireo-relevant ``models--*`` directories under the hub cache (no symlinks)."""
    from services.storage_paths import huggingface_repositories

    if not _fs().path.isdir(cache_dir):
        return []
    wanted = huggingface_repositories()
    out = []
    for name in sorted(_fs().listdir(cache_dir)):
        if name in wanted or name.startswith(("models--imageomics--", "models--bioclip--")):
            path = os.path.join(cache_dir, name)
            if _fs().path.isdir(path) and not _fs().path.islink(path):
                out.append((name, path))
    return out


def _repo_id(dir_name):
    return dir_name.removeprefix("models--").replace("--", "/")


def _onnx_repo_dir_name():
    from models import ONNX_REPO
    return "models--" + ONNX_REPO.replace("/", "--")


def _is_install_dir(path):
    """A model directory, even an empty one a download has just created."""
    return bool(path) and os.path.isdir(path)


def _within(path, root):
    """True when ``path``'s parent resolves to ``root`` or somewhere beneath it."""
    root = _fs().path.realpath(root)
    parent = _fs().path.realpath(os.path.dirname(path))
    return parent == root or parent.startswith(root + os.sep)


def _install_candidates(models_dir):
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


def _display_name(subfolder):
    import models as models_mod
    for km in models_mod.KNOWN_MODELS:
        if km.get("hf_subdir", km["id"]) == subfolder:
            return f"{km['name']} ({subfolder})"
    return subfolder


def _record(entry, path):
    """Add one snapshot file to a group, resolving a symlink to its blob."""
    entry["files"].append(path)
    target = _fs().path.realpath(path) if _fs().path.islink(path) else path
    try:
        entry["blobs"][target] = _fs().lstat(target).st_size
    except OSError:
        entry["blobs"][target] = 0
    return target if target != path else None


def _scan_repo(repo_dir, onnx):
    """Return ``(groups, referenced_blobs)`` for one repo directory.

    ``groups`` maps a group name (the ONNX subfolder, or ``""`` for a whole
    repo and for the ONNX repo's top-level files) to
    ``{"files": [snapshot paths], "blobs": {path: size}}``. Plain files
    (no-symlink layouts) are recorded as their own blob. Symlinked
    directories are not entered at any level.
    """
    groups: dict[str, dict] = {}
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


def _blob_files(repo_dir):
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


def _unreferenced_blobs(repo_dir, referenced):
    cutoff = time.time() - INCOMPLETE_GRACE_SECS
    out = {}
    for path, st in _blob_files(repo_dir).items():
        if _fs().path.realpath(path) in referenced:
            continue
        if path.endswith(".incomplete") and st.st_mtime > cutoff:
            continue
        out[_fs().path.realpath(path)] = st.st_size
    return out


def _reclaimable_blobs(group):
    """Only actual hub blobs have download locks cleanup can establish."""
    blobs_dir = _fs().path.realpath(os.path.join(group["repo_dir"], "blobs"))
    return {path: size for path, size in group["blobs"].items()
            if os.path.dirname(path) == blobs_dir}


def _size_groups(found):
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


def _collect(cache_dir, models_dir):
    """Every group across every Vireo repo, keyed, with install status."""
    from services.storage_paths import directory_bytes

    by_subfolder, by_repo = _install_candidates(models_dir)
    onnx_name = _onnx_repo_dir_name()
    total = 0
    found = {}
    for dir_name, repo_dir in _repo_dirs(cache_dir):
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
                "key": key, "name": name, "repo_dir": repo_dir,
                "files": data["files"], "blobs": data["blobs"],
                "installed": install is not None, "install_path": install,
            }
        dangling = _unreferenced_blobs(repo_dir, referenced)
        if dangling:
            key = repo + UNREFERENCED_SUFFIX
            found[key] = {
                "key": key, "name": f"{repo}: files no download refers to",
                "repo_dir": repo_dir, "files": list(dangling), "blobs": dangling,
                "installed": False, "install_path": None, "dangling": True,
            }
    _size_groups(found)
    return total, found


def _public(group):
    return {
        "key": group["key"], "name": group["name"], "size": group["size"],
        "shared_size": group["shared_size"], "installed": group["installed"],
        "install_path": group["install_path"], "file_count": len(group["files"]),
        "retained_size": group["retained_size"], "removable": group["removable"],
    }


def inventory(cache_dir=None, models_dir=None):
    """Describe the Vireo entries in the hub cache, per model.

    ``size`` is the bytes the Vireo repos occupy on disk; ``entries`` splits
    them by model with ``installed`` saying whether an install folder exists
    for it. ``unused_size`` is what ``remove_unused`` would reclaim: every
    blob the unused entries reference that no installed entry also does.
    """
    cache_dir = _cache_dir(cache_dir)
    total, found = _collect(cache_dir, _models_dir(models_dir))
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


def _remove_repo_if_empty(_repo_dir):
    """Keep directory anchors and metadata, including after the last payload.

    Even empty-directory pruning can race a rename replacement. No cleanup
    path removes directory bindings, refs, markers, or newly arriving files.
    """
    return False


def _unlink_within(root, path):
    tree = _ACTIVE_TREE.get()
    if tree is None or os.path.abspath(root) != tree.root:
        raise UnsafeCacheCleanup("Deletion requires the tree pinned during collection")
    tree.unlink(path)


def _rmdir_within(_root, _path):
    raise UnsafeCacheCleanup("Cache directory pruning is disabled")


def _download_lock_targets(found):
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


def _unlink_snapshot_files(group, failures):
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


def _delete_groups(groups):
    """Private callers cannot supply stale paths outside locked collection."""
    if _ACTIVE_TREE.get() is None:
        raise UnsafeCacheCleanup("Groups must be collected inside the cleanup transaction")
    return _delete_groups_locked(groups)


def _collect_locked(cache_dir, models_dir, locks):
    # Recollect after acquiring newly discovered targets. A late *different*
    # blob is never selected without its lock; same-etag writers must wait.
    for _attempt in range(4):
        _total, found = _collect(cache_dir, models_dir)
        targets = _download_lock_targets(found)
        if targets <= locks.held.keys():
            return found
        try:
            locks.hold(targets)
        except BlockingIOError:
            raise DownloadInProgress("another process holds a hub download lock") from None
    raise DownloadInProgress("cache download targets keep changing")


def _delete_selected(cache_dir, models_dir, select):
    """Pin first, then collect/select/delete under the same download guards."""
    try:
        acquired = DOWNLOAD_LOCK.acquire(blocking=False)
    except (ImportError, OSError) as exc:
        raise UnsafeCacheCleanup(f"Cannot establish the shared download guard: {exc}") from exc
    if not acquired:
        raise DownloadInProgress("a model download is in progress")
    try:
        if not os.path.lexists(cache_dir):
            return _result(select({}), 0, [])
        try:
            with CacheTree(cache_dir) as tree, HubLocks(tree) as locks:
                token = _ACTIVE_TREE.set(tree)
                try:
                    found = _collect_locked(cache_dir, models_dir, locks)
                    groups = select(found)
                    freed, failures = _delete_groups_locked(groups)
                    return _result(groups, freed, failures)
                finally:
                    _ACTIVE_TREE.reset(token)
        except OSError as exc:
            raise UnsafeCacheCleanup(f"Cache changed or cannot be safely accessed: {exc}") from exc

    finally:
        DOWNLOAD_LOCK.release()


def _delete_groups_locked(groups):
    freed = 0
    failures: list[dict] = []
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


def _result(groups, freed, failures):
    failed_keys = {f["key"] for f in failures}
    return {
        "removed": [g["key"] for g in groups if g["key"] not in failed_keys],
        "failed": failures,
        "freed": freed,
    }


def remove(keys, cache_dir=None, models_dir=None):
    """Remove the entries named by ``keys``.

    Unknown keys raise ``KeyError``; a key whose entry backs an installed
    model raises ``ValueError``. Both are decided on state collected under
    the download locks, not on what the page inventoried earlier.
    """
    def select(found):
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


def remove_unused(cache_dir=None, models_dir=None):
    """Remove every entry that backs no installed model."""
    result = _delete_selected(
        _cache_dir(cache_dir), _models_dir(models_dir),
        lambda found: [g for g in found.values() if not g["installed"] and g["removable"]],
    )
    log.info("Removed %d unused HF cache entries (%d bytes, %d failures)",
             len(result["removed"]) + len(result["failed"]), result["freed"], len(result["failed"]))
    return result


def _purge_keys(keys, cache_dir, models_dir):
    """Delete the entries for ``keys`` that exist and back no model directory.

    Collected under the download locks like a Storage-page removal: the
    Settings delete that calls this has just removed the install directory,
    so an entry that is installed again means a reinstall began in between,
    and its cache copy is exactly what that reinstall needs.
    """
    result = _delete_selected(
        cache_dir, models_dir,
        lambda found: [found[k] for k in dict.fromkeys(keys)
                       if k in found and not found[k]["installed"]],
    )
    if result["failed"]:
        raise CachePurgeIncomplete(result)
    return result["freed"]


def purge_outcome(operation, identifier):
    """Preserve model-removal success and report any cache cleanup failure."""
    try:
        operation(identifier)
        return {}
    except CachePurgeIncomplete as exc:
        result = exc.result
    except Exception as exc:  # Report the failure to the caller below, with a warning.
        result = {"freed": 0, "failed": [{"key": identifier, "error": str(exc)}]}
    log.warning("Model removed but HF cache cleanup incomplete for %s: %s", identifier, result)
    return {"ok": False, "cache_cleanup": result}


def purge_subfolder(subfolder, cache_dir=None, models_dir=None):
    """Drop the ONNX repo's cache entry for ``subfolder`` after its install went.

    Called after a Settings delete, so the cache copy leaves with the install
    instead of lingering invisibly. Returns the bytes freed.
    """
    from models import ONNX_REPO

    freed = _purge_keys([f"{ONNX_REPO}/{subfolder}"], _cache_dir(cache_dir), _models_dir(models_dir))
    log.info("Purged HF cache entry for %s (%d bytes)", subfolder, freed)
    return freed


def purge_model(model_id, cache_dir=None, models_dir=None):
    """Drop every cache entry a known model's download produced."""
    import models as models_mod
    from models import ONNX_REPO

    km = next((m for m in models_mod.KNOWN_MODELS if m["id"] == model_id), None)
    if km is None:
        return 0
    keys = [f"{ONNX_REPO}/{km.get('hf_subdir', model_id)}"]
    for field in ("model_str", "source"):
        value = km.get(field, "")
        if value.startswith("hf-hub:"):
            keys.append(value.removeprefix("hf-hub:"))
    return _purge_keys(keys, _cache_dir(cache_dir), _models_dir(models_dir))
