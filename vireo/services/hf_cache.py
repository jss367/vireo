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

Deletion never follows a symlink: a symlinked repository, ``snapshots`` or
revision directory is ignored, and on POSIX each unlink walks from the
validated ``snapshots/`` or ``blobs/`` directory descriptor with
``O_NOFOLLOW`` (``_unlink_within``), so a directory swapped for a symlink
between the check and the unlink cannot redirect it (Windows has no
``dir_fd``; there the pathname checks are the guard). In-process downloads
hold ``DOWNLOAD_LOCK`` across fetch-and-copy and cleanup takes it without
blocking, answering "a download is running" instead. Another process's
``hf_hub_download`` holds a ``filelock`` on ``<cache>/.locks/<repo>/<etag>.lock``
for its duration; cleanup acquires every such lock file non-blockingly and
holds them through deletion (the files outlive their locks, so presence alone
means nothing). A download that starts in another process after that
acquisition is the residual race, the same one the hub's own
``delete_revisions`` accepts with no locking at all. A repository directory
is only removed with ``rmdir`` steps that fail if anything new appears in
it, once it holds no snapshot and no blob (including a partial
``.incomplete`` download).
"""

from __future__ import annotations

import contextlib
import logging
import os
import stat
import threading
import time

log = logging.getLogger(__name__)

UNREFERENCED_SUFFIX = "#unreferenced"
# Held by every in-process model download across ``hf_hub_download`` and the
# copy into the models directory, and taken (non-blocking) by cleanup.
DOWNLOAD_LOCK = threading.RLock()


class DownloadInProgress(RuntimeError):
    """Cleanup refused because a model download may be writing the cache."""

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

    if not os.path.isdir(cache_dir):
        return []
    wanted = huggingface_repositories()
    out = []
    for name in sorted(os.listdir(cache_dir)):
        if name in wanted or name.startswith(("models--imageomics--", "models--bioclip--")):
            path = os.path.join(cache_dir, name)
            if os.path.isdir(path) and not os.path.islink(path):
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
    root = os.path.realpath(root)
    parent = os.path.realpath(os.path.dirname(path))
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
    target = os.path.realpath(path) if os.path.islink(path) else path
    try:
        entry["blobs"][target] = os.lstat(target).st_size
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
    if os.path.islink(snapshots) or not os.path.isdir(snapshots):
        return groups, referenced
    for rev in sorted(os.listdir(snapshots)):
        rev_dir = os.path.join(snapshots, rev)
        if os.path.islink(rev_dir) or not os.path.isdir(rev_dir):
            continue
        for root, dirs, files in os.walk(rev_dir):
            dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
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
    if os.path.islink(blobs_dir) or not os.path.isdir(blobs_dir):
        return {}
    out = {}
    for name in os.listdir(blobs_dir):
        path = os.path.join(blobs_dir, name)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if os.path.isfile(path) and not os.path.islink(path):
            out[path] = st
    return out


def _unreferenced_blobs(repo_dir, referenced):
    cutoff = time.time() - INCOMPLETE_GRACE_SECS
    out = {}
    for path, st in _blob_files(repo_dir).items():
        if os.path.realpath(path) in referenced:
            continue
        if path.endswith(".incomplete") and st.st_mtime > cutoff:
            continue
        out[os.path.realpath(path)] = st.st_size
    return out


def _size_groups(found):
    """Set each group's ``size`` to the bytes only it references."""
    referrers: dict[str, set[str]] = {}
    for group in found.values():
        for blob in group["blobs"]:
            referrers.setdefault(blob, set()).add(group["key"])
    for group in found.values():
        exclusive = [s for b, s in group["blobs"].items() if referrers[b] == {group["key"]}]
        group["size"] = sum(exclusive)
        group["shared_size"] = sum(group["blobs"].values()) - group["size"]


def _collect(cache_dir, models_dir):
    """Every group across every Vireo repo, keyed, with install status."""
    from services.storage_paths import directory_bytes

    by_subfolder, by_repo = _install_candidates(models_dir)
    onnx_name = _onnx_repo_dir_name()
    total = 0
    found = {}
    for dir_name, repo_dir in _repo_dirs(cache_dir):
        total += directory_bytes(repo_dir)
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
    for group in found.values():
        (installed_blobs if group["installed"] else unused_blobs).update(group["blobs"])
    ordered = sorted(found.values(), key=lambda g: (-g["size"], g["key"]))
    return {
        "path": cache_dir,
        "size": total,
        "entries": [_public(g) for g in ordered],
        "unused_size": sum(s for b, s in unused_blobs.items() if b not in installed_blobs),
        "installed_size": sum(installed_blobs.values()),
    }


def _prune_empty_dirs(path, stop):
    """Remove empty directories from ``path`` up to (not including) ``stop``."""
    stop = os.path.realpath(stop)
    while os.path.realpath(path) != stop and os.path.isdir(path):
        try:
            os.rmdir(path)
        except OSError:
            return
        path = os.path.dirname(path)


def _hub_lock_paths(repo_dir):
    locks = os.path.join(os.path.dirname(repo_dir), ".locks", os.path.basename(repo_dir))
    if os.path.islink(locks) or not os.path.isdir(locks):
        return []
    return [os.path.join(locks, n) for n in sorted(os.listdir(locks)) if n.endswith(".lock")]


@contextlib.contextmanager
def _holding_hub_locks(repo_dirs):
    """Hold every existing hub download lock for ``repo_dirs`` while deleting.

    ``hf_hub_download`` holds ``filelock`` on ``.locks/<repo>/<etag>.lock``
    for the length of a download; the file stays behind afterwards, so only
    acquiring it says whether a download is running. Raises
    ``DownloadInProgress`` without deleting anything if one is held.
    """
    try:
        from filelock import FileLock, Timeout
    except ImportError:  # pragma: no cover - filelock ships with huggingface_hub
        yield
        return
    held = []
    try:
        for repo_dir in repo_dirs:
            for path in _hub_lock_paths(repo_dir):
                lock = FileLock(path, is_singleton=False)
                try:
                    lock.acquire(timeout=0)
                except Timeout:
                    raise DownloadInProgress(
                        f"another process is downloading into {os.path.basename(repo_dir)}"
                    ) from None
                held.append(lock)
        yield
    finally:
        for lock in held:
            lock.release()


def _rmdir_tree_of_known_shape(repo_dir):
    """Remove an emptied repository with ``rmdir`` steps, never recursively.

    Only the layout the hub writes is removed: empty ``snapshots/`` and
    ``blobs/``, the small ref files under ``refs/``, the marker files under
    ``.no_exist/<rev>/``, then the repository itself. Each ``rmdir`` fails if
    anything new has appeared, in which case the repository is left in place
    and the caller reports that honestly.
    """
    def unlink_files_in(directory):
        if os.path.islink(directory) or not os.path.isdir(directory):
            return
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            if os.path.isfile(path) and not os.path.islink(path):
                _unlink_within(directory, path)
        os.rmdir(directory)

    for sub in ("snapshots", "blobs"):
        path = os.path.join(repo_dir, sub)
        if os.path.lexists(path):
            os.rmdir(path)
    unlink_files_in(os.path.join(repo_dir, "refs"))
    no_exist = os.path.join(repo_dir, ".no_exist")
    if os.path.isdir(no_exist) and not os.path.islink(no_exist):
        for rev in os.listdir(no_exist):
            unlink_files_in(os.path.join(no_exist, rev))
        os.rmdir(no_exist)
    os.rmdir(repo_dir)


def _remove_repo_if_empty(repo_dir):
    """Remove the repository directory once nothing in it is worth keeping.

    Returns True when it is gone. Any blob, a resumable ``.incomplete``
    partial included, keeps it; so does content the hub does not write.
    """
    if os.path.islink(repo_dir) or not os.path.isdir(repo_dir):
        return False
    snapshots = os.path.join(repo_dir, "snapshots")
    if (os.path.isdir(snapshots) and os.listdir(snapshots)) or _blob_files(repo_dir):
        return False
    try:
        _rmdir_tree_of_known_shape(repo_dir)
    except OSError as exc:
        log.info("Left HF cache repo %s in place: %s", repo_dir, exc)
        return False
    log.info("Removed empty HF cache repo %s", repo_dir)
    return True


_DIR_FD_UNLINK = (
    {os.open, os.unlink} <= os.supports_dir_fd
    and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY")
)


def _unlink_within(root, path):
    """Unlink ``path`` beneath ``root`` without following a symlinked directory.

    On POSIX this opens ``root`` and each intermediate directory with
    ``O_NOFOLLOW`` and unlinks by name relative to that descriptor, so a
    directory swapped for a symlink after the pathname checks cannot redirect
    the unlink outside the cache. ``path`` itself may be a symlink (a snapshot
    entry); only the link is removed.
    """
    rel = os.path.relpath(path, root)
    parts = rel.split(os.sep)
    if os.path.isabs(rel) or ".." in parts or "" in parts:
        raise OSError(f"{path} is not inside {root}")
    if not _DIR_FD_UNLINK:
        _unlink_without_dir_fd(root, parts)
        return
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(root, flags)
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        os.unlink(parts[-1], dir_fd=fd)
    finally:
        os.close(fd)


def _unlink_without_dir_fd(root, parts):
    """Windows fallback: no ``dir_fd``, so re-check every ancestor for a
    reparse point (junction or symlink) immediately before unlinking by name.
    Narrower than the descriptor walk, and the best the platform offers."""
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    current = root
    for part in [None, *parts[:-1]]:
        if part is not None:
            current = os.path.join(current, part)
        st = os.lstat(current)
        if not stat.S_ISDIR(st.st_mode) or (
            reparse and getattr(st, "st_file_attributes", 0) & reparse
        ):
            raise OSError(f"{current} is not a plain directory")
    os.unlink(os.path.join(current, parts[-1]))


def _unlink_snapshot_files(group, failures):
    """Unlink a group's snapshot entries; return the plain-file bytes freed."""
    freed = 0
    snapshots = os.path.join(group["repo_dir"], "snapshots")
    for path in group["files"]:
        if not _within(path, snapshots) or (os.path.isdir(path) and not os.path.islink(path)):
            failures.append({"key": group["key"], "path": path,
                             "error": "not a file inside the repository's snapshots"})
            continue
        try:
            size = 0 if os.path.islink(path) else os.lstat(path).st_size
            _unlink_within(snapshots, path)
            freed += size
        except OSError as exc:
            log.warning("Could not remove HF cache entry %s: %s", path, exc)
            failures.append({"key": group["key"], "path": path, "error": str(exc)})
            continue
        _prune_empty_dirs(os.path.dirname(path), snapshots)
    return freed


def _delete_groups(groups):
    """Unlink the groups' snapshot files and any blob they alone referenced.

    Returns ``(freed_bytes, failures)``; a failure names the key and path a
    deletion could not complete, so callers never report it as removed.
    Raises ``DownloadInProgress`` without deleting anything while a download
    holds ``DOWNLOAD_LOCK`` or another process holds a hub lock file.
    """
    if not DOWNLOAD_LOCK.acquire(blocking=False):
        raise DownloadInProgress("a model download is in progress")
    try:
        with _holding_hub_locks(dict.fromkeys(g["repo_dir"] for g in groups)):
            return _delete_groups_locked(groups)
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
            if _within(blob, blobs_dir) and not os.path.islink(blob):
                owners, _size = candidate_blobs.setdefault(repo_dir, {}).setdefault(
                    blob, (set(), size))
                owners.add(group["key"])
    for repo_dir, blobs in candidate_blobs.items():
        _groups, still_referenced = _scan_repo(repo_dir, onnx=True)
        blobs_root = os.path.realpath(os.path.join(repo_dir, "blobs"))
        for blob, (owners, size) in blobs.items():
            if blob in still_referenced or not os.path.isfile(blob):
                continue
            try:
                _unlink_within(blobs_root, blob)
                freed += size
            except OSError as exc:
                log.warning("Could not remove HF cache blob %s: %s", blob, exc)
                failures.extend(
                    {"key": key, "path": blob, "error": str(exc)} for key in sorted(owners))
    for repo_dir in dict.fromkeys(g["repo_dir"] for g in groups):
        _remove_repo_if_empty(repo_dir)
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
    model raises ``ValueError``, rechecked here rather than trusted from the
    page, which may have inventoried before the install finished.
    """
    _total, found = _collect(_cache_dir(cache_dir), _models_dir(models_dir))
    missing = [k for k in keys if k not in found]
    if missing:
        raise KeyError(missing[0])
    groups = [found[k] for k in dict.fromkeys(keys)]
    installed = [g["key"] for g in groups if g["installed"]]
    if installed:
        raise ValueError(installed[0])
    freed, failures = _delete_groups(groups)
    log.info("Removed HF cache entries %s (%d bytes, %d failures)", list(keys), freed, len(failures))
    return _result(groups, freed, failures)


def remove_unused(cache_dir=None, models_dir=None):
    """Remove every entry that backs no installed model."""
    _total, found = _collect(_cache_dir(cache_dir), _models_dir(models_dir))
    groups = [g for g in found.values() if not g["installed"]]
    freed, failures = _delete_groups(groups)
    log.info("Removed %d unused HF cache entries (%d bytes, %d failures)",
             len(groups), freed, len(failures))
    return _result(groups, freed, failures)


def purge_subfolder(subfolder, cache_dir=None, models_dir=None):
    """Drop the ONNX repo's cache entry for ``subfolder`` whether installed or not.

    Called after a Settings delete, so the cache copy leaves with the install
    instead of lingering invisibly. Returns the bytes freed.
    """
    from models import ONNX_REPO

    _total, found = _collect(_cache_dir(cache_dir), _models_dir(models_dir))
    group = found.get(f"{ONNX_REPO}/{subfolder}")
    if group is None:
        return 0
    freed, _failures = _delete_groups([group])
    log.info("Purged HF cache entry for %s (%d bytes)", subfolder, freed)
    return freed


def purge_model(model_id, cache_dir=None, models_dir=None):
    """Drop every cache entry a known model's download produced."""
    import models as models_mod

    km = next((m for m in models_mod.KNOWN_MODELS if m["id"] == model_id), None)
    if km is None:
        return 0
    cache_dir = _cache_dir(cache_dir)
    models_dir = _models_dir(models_dir)
    freed = purge_subfolder(km.get("hf_subdir", model_id), cache_dir, models_dir)
    _total, found = _collect(cache_dir, models_dir)
    for field in ("model_str", "source"):
        value = km.get(field, "")
        group = found.get(value.removeprefix("hf-hub:")) if value.startswith("hf-hub:") else None
        if group is not None and not group["installed"]:
            freed += _delete_groups([group])[0]
    return freed
