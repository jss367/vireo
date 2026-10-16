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
"""

from __future__ import annotations

import logging
import os
import shutil
import time

log = logging.getLogger(__name__)

UNREFERENCED_SUFFIX = "#unreferenced"
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
    """Vireo-relevant ``models--*`` directories under the hub cache."""
    from services.storage_paths import huggingface_repositories

    if not os.path.isdir(cache_dir):
        return []
    wanted = huggingface_repositories()
    out = []
    for name in sorted(os.listdir(cache_dir)):
        if name in wanted or name.startswith(("models--imageomics--", "models--bioclip--")):
            path = os.path.join(cache_dir, name)
            if os.path.isdir(path):
                out.append((name, path))
    return out


def _repo_id(dir_name):
    return dir_name.removeprefix("models--").replace("--", "/")


def _onnx_repo_dir_name():
    from models import ONNX_REPO
    return "models--" + ONNX_REPO.replace("/", "--")


def _has_files(path):
    if not path or not os.path.isdir(path):
        return False
    return any(files for _root, _dirs, files in os.walk(path))


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
    (no-symlink layouts) are recorded as their own blob.
    """
    groups: dict[str, dict] = {}
    referenced: set[str] = set()
    snapshots = os.path.join(repo_dir, "snapshots")
    if not os.path.isdir(snapshots):
        return groups, referenced
    for rev in sorted(os.listdir(snapshots)):
        rev_dir = os.path.join(snapshots, rev)
        if not os.path.isdir(rev_dir):
            continue
        for root, _dirs, files in os.walk(rev_dir):
            for name in files:
                path = os.path.join(root, name)
                rel = os.path.relpath(path, rev_dir)
                group = rel.split(os.sep, 1)[0] if onnx and os.sep in rel else ""
                entry = groups.setdefault(group, {"files": [], "blobs": {}})
                blob = _record(entry, path)
                if blob:
                    referenced.add(blob)
    return groups, referenced


def _unreferenced_blobs(repo_dir, referenced):
    blobs_dir = os.path.join(repo_dir, "blobs")
    if not os.path.isdir(blobs_dir):
        return {}
    cutoff = time.time() - INCOMPLETE_GRACE_SECS
    out = {}
    for name in os.listdir(blobs_dir):
        path = os.path.join(blobs_dir, name)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if os.path.realpath(path) in referenced or not os.path.isfile(path):
            continue
        if name.endswith(".incomplete") and st.st_mtime > cutoff:
            continue
        out[os.path.realpath(path)] = st.st_size
    return out


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
            install = next((d for d in candidates if _has_files(d)), None)
            found[key] = {
                "key": key, "name": name, "repo_dir": repo_dir,
                "files": data["files"], "blobs": data["blobs"],
                "size": sum(data["blobs"].values()),
                "installed": install is not None, "install_path": install,
            }
        dangling = _unreferenced_blobs(repo_dir, referenced)
        if dangling:
            key = repo + UNREFERENCED_SUFFIX
            found[key] = {
                "key": key, "name": f"{repo}: files no download refers to",
                "repo_dir": repo_dir, "files": list(dangling), "blobs": dangling,
                "size": sum(dangling.values()), "installed": False, "install_path": None,
                "dangling": True,
            }
    return total, found


def _public(group):
    return {
        "key": group["key"], "name": group["name"], "size": group["size"],
        "installed": group["installed"], "install_path": group["install_path"],
        "file_count": len(group["files"]),
    }


def inventory(cache_dir=None, models_dir=None):
    """Describe the Vireo entries in the hub cache, per model.

    ``size`` is the bytes the Vireo repos occupy on disk; ``entries`` splits
    them by model with ``installed`` saying whether an install folder exists
    for it, so ``unused_size`` is what ``remove_unused`` would reclaim.
    """
    cache_dir = _cache_dir(cache_dir)
    total, found = _collect(cache_dir, _models_dir(models_dir))
    ordered = sorted(found.values(), key=lambda g: (-g["size"], g["key"]))
    entries = [_public(g) for g in ordered]
    return {
        "path": cache_dir,
        "size": total,
        "entries": entries,
        "unused_size": sum(e["size"] for e in entries if not e["installed"]),
        "installed_size": sum(e["size"] for e in entries if e["installed"]),
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


def _delete_groups(groups):
    """Unlink the groups' snapshot files and any blob they alone referenced."""
    freed = 0
    candidate_blobs: dict[str, dict[str, int]] = {}
    for group in groups:
        repo_dir = group["repo_dir"]
        blobs_root = os.path.realpath(os.path.join(repo_dir, "blobs")) + os.sep
        for path in [] if group.get("dangling") else group["files"]:
            try:
                if not os.path.islink(path):
                    freed += os.lstat(path).st_size
                os.unlink(path)
            except OSError as exc:
                log.warning("Could not remove HF cache entry %s: %s", path, exc)
                continue
            _prune_empty_dirs(os.path.dirname(path), os.path.join(repo_dir, "snapshots"))
        for blob, size in group["blobs"].items():
            if os.path.realpath(blob).startswith(blobs_root):
                candidate_blobs.setdefault(repo_dir, {})[os.path.realpath(blob)] = size
    for repo_dir, blobs in candidate_blobs.items():
        _groups, still_referenced = _scan_repo(repo_dir, onnx=True)
        for blob, size in blobs.items():
            if blob in still_referenced or not os.path.isfile(blob):
                continue
            try:
                os.unlink(blob)
                freed += size
            except OSError as exc:
                log.warning("Could not remove HF cache blob %s: %s", blob, exc)
    for repo_dir in dict.fromkeys(g["repo_dir"] for g in groups):
        snapshots = os.path.join(repo_dir, "snapshots")
        if os.path.isdir(repo_dir) and (
            not os.path.isdir(snapshots) or not os.listdir(snapshots)
        ):
            shutil.rmtree(repo_dir, ignore_errors=True)
            log.info("Removed empty HF cache repo %s", repo_dir)
    return freed


def remove(keys, cache_dir=None, models_dir=None):
    """Remove the entries named by ``keys``. Unknown keys raise ``KeyError``."""
    _total, found = _collect(_cache_dir(cache_dir), _models_dir(models_dir))
    missing = [k for k in keys if k not in found]
    if missing:
        raise KeyError(missing[0])
    groups = [found[k] for k in dict.fromkeys(keys)]
    freed = _delete_groups(groups)
    log.info("Removed HF cache entries %s (%d bytes)", list(keys), freed)
    return {"removed": [g["key"] for g in groups], "freed": freed}


def remove_unused(cache_dir=None, models_dir=None):
    """Remove every entry that backs no installed model."""
    _total, found = _collect(_cache_dir(cache_dir), _models_dir(models_dir))
    groups = [g for g in found.values() if not g["installed"]]
    freed = _delete_groups(groups)
    log.info("Removed %d unused HF cache entries (%d bytes)", len(groups), freed)
    return {"removed": [g["key"] for g in groups], "freed": freed}


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
    freed = _delete_groups([group])
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
            freed += _delete_groups([group])
    return freed
