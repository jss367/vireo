"""Inspect and bound disposable retained renders and portable computations.

Originals, offline/local copies, recovery staging, edit masks, trust settings
and user exports are never candidates. Cleanup unlinks individual verified
files, never directory trees or symlinks. Failed unlinks remain measurable.
"""

import hashlib
import os
import re
import stat
import threading
import time
from functools import wraps

FAMILIES = {
    "full-resolution": ("Full-resolution renders", "originals",
                        r"[0-9]+(?:[._][^.]+)*\.jpg"),
    "external-editor": ("External editor renders", "external-edits", r"[0-9]+\.(?:jpg|json)"),
    "external-dng": ("External editor conversions", "external-dng", r".+\.dng"),
    "inat-uploads": ("iNaturalist upload renders", "inat-uploads", r"[0-9]+\.(?:jpg|json)"),
    "computation": ("Saved computation results", None, r"[0-9a-f]{64}\.json"),
}
WRITE_GRACE_SECONDS = 3600
# External applications can save unique changes into either handoff. A
# readable original cannot prove those changes are recoverable.
PRESERVED_FAMILIES = frozenset({"external-editor", "external-dng"})
_READ_GUARDS = {family: threading.RLock() for family in FAMILIES}


def reader_guard(family):
    """Pin a family's paths through validation, publication and file open.

    Acquire after source/preparation locks, never around artifact-flight
    waits. File responses open eagerly; their descriptor pins the bytes
    after this guard is released. External applications are not covered,
    which is why their handoffs are never cleanup candidates.
    """
    return _READ_GUARDS[family]


def guarded_reader(family):
    def decorate(function):
        @wraps(function)
        def read(*args, **kwargs):
            with reader_guard(family):
                return function(*args, **kwargs)
        return read
    return decorate


def source_verifier(db, config, checkpoint=lambda: None):
    """Fail closed unless independent catalog sources can be read in full."""
    generated = os.path.dirname(config["THUMB_CACHE_DIR"])
    excluded = [*roots(config).values(), *[os.path.join(generated, name) for name in (
        "offline", "working", "previews", "staging", "local-folders", "local-workspaces",
    )], config["THUMB_CACHE_DIR"]]
    excluded = [os.path.normcase(os.path.realpath(path)) for path in excluded]

    def verify(path):
        match = re.match(r"([0-9]+)[._]", os.path.basename(path))
        if not match:
            return False
        photo_id = int(match[1])
        if not 0 < photo_id < 2**63:
            return False
        source = db.storage.retained_source(photo_id)
        if not source or not source["file_hash"]:
            return False
        names = [source["filename"]]
        if source["companion_path"]:
            names.append(source["companion_path"])
        try:
            for index, name in enumerate(names):
                original = os.path.join(source["folder_path"], name)
                resolved = os.path.normcase(os.path.realpath(original))
                if any(_within(resolved, root) for root in excluded):
                    return False
                with open(original, "rb") as handle:
                    before = os.fstat(handle.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                        return False
                    if index == 0 and (before.st_size != source["file_size"] or before.st_mtime != source["file_mtime"]):
                        return False
                    digest = hashlib.sha256()
                    while block := handle.read(1024 * 1024):
                        checkpoint()
                        digest.update(block)
                    if _identity(before) != _identity(os.fstat(handle.fileno())):
                        return False
                    if index == 0 and source["file_hash"] and digest.hexdigest() != source["file_hash"]:
                        return False
                if _identity(before) != _identity(os.stat(original)):
                    return False
            return db.storage.retained_source(photo_id) == source
        except (OSError, ValueError, TypeError):
            return False
    return verify


def roots(config):
    generated = os.path.dirname(config["THUMB_CACHE_DIR"])
    return {key: os.path.join(generated, subdir) if subdir else
            os.path.join(config["COMPUTATION_CACHE_DIR"], "objects")
            for key, (_label, subdir, _pattern) in FAMILIES.items()}


def files(root, family, *, onerror=None):
    """Yield exact generated payload files without following symlink trees."""
    if os.path.islink(root):
        if onerror:
            onerror(PermissionError(f"Symbolic-link cache directory is excluded: {root}"))
        return
    pattern = re.compile(FAMILIES[family][2])
    def walk_error(exc):
        if not isinstance(exc, FileNotFoundError) and onerror:
            onerror(exc)

    for directory, dirs, names in os.walk(root, onerror=walk_error):
        dirs[:] = [name for name in dirs if not os.path.islink(os.path.join(directory, name))]
        for name in names:
            if not pattern.fullmatch(name):
                continue
            path = os.path.join(directory, name)
            try:
                info = os.lstat(path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                if onerror:
                    onerror(exc)
                continue
            if stat.S_ISREG(info.st_mode):
                yield path, info


def summary(config, *, limit=100):
    result = []
    for family, root in roots(config).items():
        count = size = 0
        entries = []
        errors = []
        for path, info in files(root, family, onerror=lambda exc, errors=errors: errors.append(str(exc))):
            count += 1
            size += info.st_size
            if len(entries) < limit:
                entries.append({"name": os.path.relpath(path, root), "size": info.st_size,
                                "modified_at": info.st_mtime})
        result.append({"type": family, "name": FAMILIES[family][0], "path": root,
                       "count": count, "size": size, "files": entries,
                       "clearable": family not in PRESERVED_FAMILIES,
                       "truncated": count > len(entries), "errors": errors})
    return result


def cleanup(config, source_folders, *, family=None, clear=False, checkpoint=lambda: None,
            progress=lambda current, total, path: None, now=None, source_is_readable=None):
    """Enforce a shared payload budget/age, or explicitly clear one family.

    Automatic maintenance gives publications an hour of grace. Policies
    cover payloads shared by every workspace. Size is payload bytes; folder
    allocation, ignored files and computation trust metadata are separate.
    """
    if family is not None and family not in FAMILIES:
        raise ValueError("Unknown retained cache type")
    if family in PRESERVED_FAMILIES:
        raise ValueError("External editor files may contain unique edits. Save a separate copy before removing them manually.")
    now = time.time() if now is None else now
    max_bytes = int(config.get("retained_cache_max_mb", 10240)) * 1024**2
    age_days = int(config.get("retained_cache_max_age_days", 30))
    selected = roots(config)
    source_folders = [os.path.normcase(os.path.realpath(path)) for path in source_folders if path]
    candidates = []
    errors = []
    errors_total = 0
    def add_error(message):
        nonlocal errors_total
        errors_total += 1
        if len(errors) < 50:
            errors.append(str(message))

    for key, root in selected.items():
        if key in PRESERVED_FAMILIES:
            continue
        if family and key != family:
            continue
        resolved = os.path.normcase(os.path.realpath(root))
        if any(_within(path, resolved) for path in source_folders):
            add_error(f"Skipped {root}: a catalog photo folder uses this directory.")
            continue
        if os.path.islink(root):
            add_error(f"Skipped {root}: cache directory is a symbolic link.")
            continue
        for path, info in files(root, key, onerror=add_error):
            checkpoint()
            # Renderers deliberately peg mtime to the original's capture/
            # source time. ctime protects a just-published render even when
            # both source mtime and copied atime are years old.
            candidates.append((max(info.st_mtime, info.st_atime, info.st_ctime), path, info, resolved, key))
    total_bytes = sum(info.st_size for _access, _path, info, _root, _family in candidates)
    removed = freed = 0
    kept_unverified = 0
    # Oldest access first; fresh file publications are protected even when
    # the user lowers a limit below one very large render.
    for current, (access, path, expected, approved_root, key) in enumerate(sorted(candidates), 1):
        checkpoint()
        expired = age_days > 0 and access < now - age_days * 86400
        eligible = clear or (max(expected.st_mtime, expected.st_ctime) < now - WRITE_GRACE_SECONDS and
                             (expired or total_bytes > max_bytes))
        if not eligible:
            continue
        progress(current, len(candidates), path)
        if not clear and key != "computation" and (source_is_readable is None or not source_is_readable(path)):
            kept_unverified += 1
            continue
        try:
            with reader_guard(key):
                actual = os.lstat(path)
                if _identity(actual) != _identity(expected) or not stat.S_ISREG(actual.st_mode):
                    continue
                if not clear and actual.st_atime > expected.st_atime:
                    continue
                # Recheck containment to reject a directory replaced by a link
                # between the walk and unlink.
                if not _within(os.path.normcase(os.path.realpath(path)), approved_root):
                    continue
                os.unlink(path)
            removed += 1
            freed += expected.st_size
            total_bytes -= expected.st_size
        except FileNotFoundError:
            total_bytes -= expected.st_size
        except OSError as exc:
            add_error(f"Could not remove {path}: {exc}")
    return {"deleted": removed, "freed_bytes": freed, "remaining_bytes": total_bytes,
            "kept_unverified_sources": kept_unverified,
            "errors": errors, "errors_total": errors_total, "ok": not errors_total}


def _within(path, root):
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


def _identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
