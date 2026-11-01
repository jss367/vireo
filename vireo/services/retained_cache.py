"""Inspect and bound disposable retained renders and portable computations.

Originals, offline/local copies, recovery staging, edit masks, trust settings
and user exports are never candidates. Cleanup unlinks individual verified
files, never directory trees or symlinks. Failed unlinks remain measurable.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import threading
import time
from functools import wraps
from typing import TYPE_CHECKING, Any

from app_paths import media_root
from config import config_value
from source_reader import open_source

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from typing import BinaryIO

    from db import Database
    from services.service_types import JsonDict

    # Any: config values are heterogeneous settings (paths, limits, flags).
    _Config = Mapping[str, Any]
    # ``(st_dev, st_ino, st_size, st_mtime_ns, st_ctime_ns)``.
    _Identity = tuple[int, int, int, int, int]

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


def reader_guard(family: str) -> threading.RLock:
    """Pin a family's paths through validation, publication and file open.

    Acquire after source/preparation locks, never around artifact-flight
    waits. File responses open eagerly; their descriptor pins the bytes
    after this guard is released. External applications are not covered,
    which is why their handoffs are never cleanup candidates.
    """
    return _READ_GUARDS[family]


def guarded_reader[**P, R](family: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def read(*args: P.args, **kwargs: P.kwargs) -> R:
            with reader_guard(family):
                return function(*args, **kwargs)
        return read
    return decorate


class SourceVerifier:
    """Separate expensive source verification from the final guarded probe.

    verification_failure describes the most recent full-source rejection;
    it is empty after successful verification.
    """

    def __init__(
        self, verify: Callable[[str], bool], recheck: Callable[[str], bool], failure: JsonDict | None = None,
    ) -> None:
        self._verify = verify
        self.recheck = recheck
        self.verification_failure = {} if failure is None else failure

    def __call__(self, path: str) -> bool:
        return self._verify(path)


def _source_hash(handle: BinaryIO, checkpoint: Callable[[], object]) -> str:
    digest = hashlib.sha256()
    while True:
        checkpoint()
        block = handle.read(1024 * 1024)
        if not block:
            return digest.hexdigest()
        digest.update(block)


def source_verifier(db: Database, config: _Config, checkpoint: Callable[[], object] = lambda: None,
                    *, read_checkpoint: Callable[[], object] | None = None) -> SourceVerifier:
    """Fail closed unless independent catalog sources can be read in full."""
    # Yield before opening each source. Hashing only polls cancellation: a
    # transfer must never wait for a Windows source handle we parked with open.
    # The guarded final recheck likewise never waits under a renderer lock.
    read_checkpoint = checkpoint if read_checkpoint is None else read_checkpoint
    generated = media_root(config["THUMB_CACHE_DIR"])
    excluded = [*roots(config).values(), *[os.path.join(generated, name) for name in (
        "offline", "working", "previews", "staging", "local-folders", "local-workspaces",
    )], config["THUMB_CACHE_DIR"]]
    excluded = [os.path.normcase(os.path.realpath(path)) for path in excluded]
    verified = {}
    failure = {}

    def reject(reason: str, **details: object) -> bool:
        failure.update(reason=reason, **details)
        return False

    def path_identity(path: str) -> _Identity:
        # Windows path stat exposes creation time as ctime, while fstat
        # can expose ChangeTime. Keep every identity field, including the
        # change-time guard, by querying the named path through fstat too.
        if os.name == "nt":
            with open_source(path) as handle:
                return _identity(os.fstat(handle.fileno()))
        return _identity(os.stat(path))

    def verify(path: str) -> bool:
        failure.clear()
        verified.pop(path, None)
        match = re.match(r"([0-9]+)[._]", os.path.basename(path))
        if not match:
            return reject("unrecognized-render-name")
        photo_id = int(match[1])
        if not 0 < photo_id < 2**63:
            return reject("invalid-photo-id")
        source = db.storage.retained_source(photo_id)
        if not source or not source["file_hash"]:
            return reject("missing-source-hash", source_found=bool(source))
        identities = [(source["filename"], source["file_size"], source["file_mtime"], source["file_hash"])]
        if source["companion_path"]:
            if not source["companion_hash"]:
                return reject("missing-companion-hash")
            identities.append((source["companion_path"], source["companion_size"],
                               source["companion_mtime"], source["companion_hash"]))
        checked = []
        try:
            for name, expected_size, expected_mtime, expected_hash in identities:
                read_checkpoint()
                original = os.path.join(source["folder_path"], name)
                resolved = os.path.normcase(os.path.realpath(original))
                if any(_within(resolved, root) for root in excluded):
                    return reject("managed-source-path")
                with open_source(original) as handle:
                    before = os.fstat(handle.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                        return reject("nonempty-regular-source-required", mode=before.st_mode, size=before.st_size)
                    if before.st_size != expected_size or before.st_mtime != expected_mtime:
                        return reject("catalog-metadata-mismatch", expected_size=expected_size,
                                      actual_size=before.st_size, expected_mtime=expected_mtime,
                                      actual_mtime=before.st_mtime)
                    digest = _source_hash(handle, checkpoint)
                    after = os.fstat(handle.fileno())
                    if _identity(before) != _identity(after):
                        return reject("source-changed-during-read", before=_identity(before), after=_identity(after))
                    if digest != expected_hash:
                        return reject("source-hash-mismatch")
                after = path_identity(original)
                if _identity(before) != after:
                    return reject("source-path-identity-mismatch", before=_identity(before), after=after)
                checked.append((original, before))
            if db.storage.retained_source(photo_id) != source:
                return reject("catalog-source-changed")
            verified[path] = (photo_id, source, checked)
            return True
        except (OSError, ValueError, TypeError) as exc:
            return reject("source-verification-error", error_type=type(exc).__name__, error=str(exc))

    def recheck(path: str) -> bool:
        proof = verified.pop(path, None)
        if not proof:
            return False
        photo_id, source, checked = proof
        try:
            for original, expected in checked:
                checkpoint()
                resolved = os.path.normcase(os.path.realpath(original))
                if any(_within(resolved, root) for root in excluded):
                    return False
                with open_source(original) as handle:
                    if (_identity(os.fstat(handle.fileno())) != _identity(expected)
                            or not handle.read(1)
                            or path_identity(original) != _identity(expected)):
                        return False
            return db.storage.retained_source(photo_id) == source
        except (OSError, ValueError, TypeError):
            return False
    return SourceVerifier(verify, recheck, failure)


def roots(config: _Config) -> dict[str, str]:
    generated = media_root(config["THUMB_CACHE_DIR"])
    return {key: os.path.join(generated, subdir) if subdir else
            os.path.join(config["COMPUTATION_CACHE_DIR"], "objects")
            for key, (_label, subdir, _pattern) in FAMILIES.items()}


def files(
    root: str, family: str, *, onerror: Callable[[OSError], object] | None = None,
) -> Iterator[tuple[str, os.stat_result]]:
    """Yield exact generated payload files without following symlink trees."""
    if os.path.islink(root):
        if onerror:
            onerror(PermissionError(f"Symbolic-link cache directory is excluded: {root}"))
        return
    pattern = re.compile(FAMILIES[family][2])
    def walk_error(exc: OSError) -> None:
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


def summary(config: _Config, *, limit: int = 100) -> list[JsonDict]:
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


def cleanup(config: _Config, source_folders: Iterable[str], *, family: str | None = None, clear: bool = False,
            checkpoint: Callable[[], object] = lambda: None,
            progress: Callable[[int, int, str], object] = lambda current, total, path: None,
            now: float | None = None, source_is_readable: Callable[[str], bool] | None = None) -> JsonDict:
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
    max_bytes = int(config_value(config, "retained_cache_max_mb")) * 1024**2
    age_days = int(config_value(config, "retained_cache_max_age_days"))
    selected = roots(config)
    source_folders = [os.path.normcase(os.path.realpath(path)) for path in source_folders if path]
    candidates = []
    errors = []
    errors_total = 0
    def add_error(message: object) -> None:
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
        if any(_within(path, resolved) or _within(resolved, path) for path in source_folders):
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
    kept_changed = kept_accessed = kept_unsafe = 0
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
                    kept_changed += 1
                    continue
                if not clear and actual.st_atime > expected.st_atime:
                    kept_accessed += 1
                    continue
                # Recheck containment to reject a directory replaced by a link
                # between the walk and unlink.
                if not _within(os.path.normcase(os.path.realpath(path)), approved_root):
                    kept_unsafe += 1
                    continue
                if not clear and key != "computation":
                    # Uploads can hold this guard while consuming a file.
                    # Availability or identity may have changed since the
                    # full source read, so probe again after any lock wait.
                    recheck = getattr(source_is_readable, "recheck", source_is_readable)
                    if recheck is None or not recheck(path):
                        kept_unverified += 1
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
            "kept_changed_files": kept_changed, "kept_accessed_files": kept_accessed,
            "kept_unsafe_paths": kept_unsafe,
            "errors": errors, "errors_total": errors_total, "ok": not errors_total}


def _within(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


def _identity(info: os.stat_result) -> _Identity:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
