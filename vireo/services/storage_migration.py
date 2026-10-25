"""Offline, opt-in storage relocation using renames and compatibility links.

No catalog values or photo bytes are rewritten. The old paths keep resolving
through links, including custom paths saved inside the catalog or settings.
A recovery journal blocks startup until every move has completed or rolled back.
Cross-volume moves are refused rather than silently copying many GB of data.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from app_paths import migration_journal, native_roots
from file_replace import replace_file

if TYPE_CHECKING:
    from collections.abc import Iterator

    from services.service_types import JsonDict

MARKER = "storage-layout-v1"
CACHES = frozenset({"models", "thumbnails", "embedding_cache", "computation-cache"})
_CACHES_NORMALIZED = frozenset(os.path.normcase(name) for name in CACHES)
GUARDS = frozenset({"runtime.lock", "hf-cache-access.lock"})


def _case_insensitive(path: Path) -> bool:
    """Return True when ``path``'s filesystem matches names case-insensitively.

    ``os.path.normcase`` only lowercases on Windows, so a POSIX host with a
    case-insensitive volume (macOS APFS/HFS+ by default) needs a filesystem
    probe. Toggle the leaf's casing against the parent and compare inodes:
    the toggled lookup resolves to the same entry only when the filesystem
    folds case.
    """
    swapped = path.name.swapcase()
    if swapped == path.name:
        return False
    try:
        return path.samefile(path.parent / swapped)
    except OSError:
        return False


def _is_cache_name(name: str, *, case_insensitive: bool = False) -> bool:
    """Membership check that respects the filesystem's case sensitivity.

    On a case-insensitive filesystem (Windows always; macOS by default), a
    legacy ``Models`` folder resolves to the same path as ``models`` and
    must route to the cache root; routing it to the data root would strand
    its installed models after the native-layout marker takes effect. On a
    case-sensitive filesystem, ``Models`` is a distinct user-created
    directory and still goes to the data root.
    """
    normalized = os.path.normcase(name)
    if case_insensitive:
        normalized = normalized.lower()
    return normalized in _CACHES_NORMALIZED


class MigrationError(ValueError):
    """A migration cannot safely proceed; source data must be preserved."""


def _paths() -> tuple[Path, Path, Path, Path]:
    data, cache = native_roots()
    return Path.home() / ".vireo", Path(data), Path(cache), Path(migration_journal())


def _identity(path: Path) -> list[int]:
    info = path.lstat()
    return [info.st_dev, info.st_ino]


def _exists(path: Path) -> bool:
    return os.path.lexists(path)


def _sync_directory(path: Path) -> None:
    """Persist recovery ordering on platforms with directory fsync."""
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _validate_roots(source: Path, data: Path, cache: Path) -> None:
    if source.is_symlink() or not source.is_dir():
        raise MigrationError("Migration requires an existing, non-symlink ~/.vireo directory.")
    if data.is_symlink() or cache.is_symlink():
        raise MigrationError("Storage destination roots must not be symlinks.")
    old, new, disposable = (p.resolve() for p in (source, data, cache))
    for root in (new, disposable):
        if root == old or old in root.parents or root in old.parents:
            raise MigrationError("Storage destinations overlap the legacy directory.")
        allowed = {disposable} if root == new and disposable.parent == new else set()
        if _exists(root) and (root.is_symlink() or not root.is_dir()
                              or any(entry not in allowed for entry in root.iterdir())):
            raise MigrationError(f"Destination must be absent or empty: {root}")
        ancestor = root
        while not ancestor.exists():
            ancestor = ancestor.parent
        if ancestor.stat().st_dev != source.stat().st_dev:
            raise MigrationError(f"Cross-volume migration is unsupported: {root}")
    if new == disposable or disposable in new.parents:
        raise MigrationError("The data directory cannot be inside the cache directory.")
    if new in disposable.parents and disposable.parent != new:
        raise MigrationError("A nested cache directory must be directly inside the data directory.")
    if new in disposable.parents and _exists(source / disposable.name):
        raise MigrationError(f"Legacy data conflicts with the cache directory: {disposable.name}")
    if _exists(source / MARKER):
        raise MigrationError("Unexpected storage marker inside the legacy directory.")


def _check_tree(source: Path) -> None:
    """Reject links that change meaning when relocated, and special files."""
    def walk_error(error: OSError) -> None:
        raise error

    for root, dirs, files in os.walk(source, followlinks=False, onerror=walk_error):
        for name in dirs + files:
            path = Path(root) / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                if not os.path.isabs(os.readlink(path)):
                    raise MigrationError(f"Relative symlink requires manual review before migration: {path}")
            elif not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise MigrationError(f"Special file requires manual review before migration: {path}")
    for name in CACHES:
        path = source / name
        if _exists(path) and (path.is_symlink() or not path.is_dir()):
            raise MigrationError(f"Custom cache location requires manual migration: {path}")


def preview() -> JsonDict:
    """Inspect the plan without creating directories, locks, or journal files."""
    source, data, cache, journal = _paths()
    result: JsonDict = {"source": str(source), "data": str(data), "cache": str(cache)}
    if _exists(journal):
        return {**result, "status": "unfinished", "journal": str(journal)}
    if _exists(data / MARKER):
        return {**result, "status": "native", "message": "Platform storage is already selected."}
    _validate_roots(source, data, cache)
    _check_tree(source)
    return {**result, "status": "ready", "entries": sorted(p.name for p in source.iterdir()),
            "message": "Quit all Vireo instances before applying. Legacy paths remain as compatibility links."}


@contextmanager
def _runtime_guard(path: Path) -> Iterator[None]:
    # Use the exact lock protocol used by the server, without touching its
    # global descriptor or deleting/truncating another process's diagnostic PID.
    from runtime import _release_lock, _try_take_lock

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        if not _try_take_lock(fd):
            raise MigrationError("Vireo is running. Quit the desktop app and all Vireo servers first.")
        try:
            yield
        finally:
            _release_lock(fd)
    finally:
        os.close(fd)


def _write_journal(path: Path, record: JsonDict) -> None:
    fd, temporary = tempfile.mkstemp(prefix=path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(record, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        replace_file(temporary, path)
        _sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _record(source: Path, data: Path, cache: Path) -> JsonDict:
    return {"version": 1, "source": str(source), "data": str(data), "cache": str(cache),
            "roots": [_identity(root) for root in (source, data, cache)],
            "entries": [{"name": p.name, "identity": _identity(p), "directory": p.is_dir()}
                        for p in sorted(source.iterdir()) if p.name not in GUARDS]}


def _read_journal(journal: Path, source: Path, data: Path, cache: Path) -> JsonDict:
    record = json.loads(journal.read_text(encoding="utf-8"))
    if not isinstance(record, dict) or any(record.get(k) != v for k, v in
            {"version": 1, "source": str(source), "data": str(data), "cache": str(cache)}.items()):
        raise MigrationError("Recovery paths differ from the journal; restore the original environment first.")
    if any(root.is_symlink() or not root.is_dir() for root in (source, data, cache)) or record.get("roots") != [
            _identity(root) for root in (source, data, cache)]:
        raise MigrationError("A storage root changed after migration began; inspect the journal before recovery.")
    rows = record.get("entries")
    if not isinstance(rows, list):
        raise MigrationError("Invalid migration journal.")
    names = set()
    for row in rows:
        name = row.get("name") if isinstance(row, dict) else None
        if (not isinstance(name, str) or not name or name in {".", "..", MARKER} | GUARDS
                or Path(name).name != name or "/" in name or "\\" in name or name in names
                or not isinstance(row.get("identity"), list) or len(row["identity"]) != 2
                or not all(isinstance(n, int) for n in row["identity"])
                or not isinstance(row.get("directory"), bool)):
            raise MigrationError("Invalid entry in migration journal; no recovery changes made.")
        names.add(name)
    return record


def _target(name: str, data: Path, cache: Path, *, case_insensitive: bool = False) -> Path:
    return (cache if _is_cache_name(name, case_insensitive=case_insensitive) else data) / name


def _normalized_link_target(target: str) -> str:
    # Windows readlink returns the substitution path, including its extended
    # prefix. Compare lexical targets without following links: recovery must
    # also recognize dangling links and still reject a link to another path.
    if os.name == "nt":
        if target[:8].upper() == "\\\\?\\UNC\\":
            target = "\\\\" + target[8:]
        elif target.startswith("\\\\?\\"):
            target = target[4:]
    return os.path.normcase(target)


def _is_link(path: Path, target: Path) -> bool:
    return path.is_symlink() and _normalized_link_target(os.readlink(path)) == _normalized_link_target(str(target))


def _verify_entry(row: JsonDict, source: Path, target: Path) -> bool:
    """Return whether the original inode has moved; refuse ambiguous recovery."""
    if _exists(target):
        if _identity(target) != row["identity"] or (_exists(source) and not _is_link(source, target)):
            raise MigrationError(f"Migration path changed; preserve both locations and inspect: {source}, {target}")
        return True
    if not _exists(source) or _identity(source) != row["identity"]:
        raise MigrationError(f"Original entry changed or is missing: {source}")
    return False


def _link(path: Path, target: Path, directory: bool) -> None:
    if not _exists(path):
        path.symlink_to(target, target_is_directory=directory)
        _sync_directory(path.parent)
    elif not _is_link(path, target):
        raise MigrationError(f"Compatibility link conflicts with an existing path: {path}")


def _apply(record: JsonDict, source: Path, data: Path, cache: Path, journal: Path) -> None:
    data.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    case_insensitive = _case_insensitive(source)
    # Keep lock inodes at their original paths throughout the move and after
    # migration, so old and new servers contend for the same kernel locks.
    for name in GUARDS:
        _link(data / name, source / name, False)
    for row in record["entries"]:
        old = source / row["name"]
        target = _target(row["name"], data, cache, case_insensitive=case_insensitive)
        if not _verify_entry(row, old, target):
            old.rename(target)
            _sync_directory(old.parent)
            _sync_directory(target.parent)
        if _identity(target) != row["identity"]:
            raise MigrationError(f"Moved entry could not be verified: {target}")
        _link(old, target, row["directory"])
        if _is_cache_name(row["name"], case_insensitive=case_insensitive):
            _link(data / row["name"], target, True)
    marker = data / MARKER
    if not _exists(marker):
        with marker.open("x", encoding="utf-8") as stream:
            stream.write("Migrated with compatibility links; keep ~/.vireo.\n")
            stream.flush()
            os.fsync(stream.fileno())
    _sync_directory(data)
    journal.unlink()
    _sync_directory(journal.parent)


def _rollback(record: JsonDict, source: Path, data: Path, cache: Path, journal: Path) -> None:
    case_insensitive = _case_insensitive(source)
    # Validate every row before touching anything, so changed paths stop recovery.
    for row in record["entries"]:
        _verify_entry(row, source / row["name"],
                      _target(row["name"], data, cache, case_insensitive=case_insensitive))
    for row in reversed(record["entries"]):
        old = source / row["name"]
        target = _target(row["name"], data, cache, case_insensitive=case_insensitive)
        if _exists(target):
            if old.is_symlink():
                old.unlink()
            target.rename(old)
            _sync_directory(old.parent)
            _sync_directory(target.parent)
        if _is_cache_name(row["name"], case_insensitive=case_insensitive) and _is_link(data / row["name"], target):
            (data / row["name"]).unlink()
    for name in GUARDS:
        if _is_link(data / name, source / name):
            (data / name).unlink()
    if _exists(data / MARKER):
        (data / MARKER).unlink()
    _sync_directory(data)
    journal.unlink()
    _sync_directory(journal.parent)


def migrate(*, rollback: bool = False) -> JsonDict:
    """Move the offline legacy install or recover its unfinished migration."""
    from filelock import FileLock, Timeout
    from services.hf_cache_lock import SharedDownloadLock

    source, data, cache, journal = _paths()
    if not _exists(journal):
        if rollback:
            raise MigrationError("There is no unfinished migration to roll back.")
        plan = preview()
        if plan["status"] == "native":
            return plan
    if source.is_symlink() or not source.is_dir():
        raise MigrationError("Legacy directory is missing or has become a symlink.")
    with ExitStack() as stack:
        try:
            stack.enter_context(FileLock(str(journal) + ".lock", timeout=0))
        except Timeout:
            raise MigrationError("Another storage migration is running.") from None
        stack.enter_context(_runtime_guard(source / "runtime.lock"))
        if not _exists(journal) and preview()["status"] == "native":
            return preview()
        downloads = SharedDownloadLock(str(source / "hf-cache-access.lock"))
        if not downloads.acquire(blocking=False):
            raise MigrationError("A model/cache operation is running; wait for it to finish.")
        stack.callback(downloads.release)
        if _exists(journal):
            record = _read_journal(journal, source, data, cache)
        else:
            preview()  # Recheck collisions and symlinks under the guards.
            data.mkdir(parents=True, exist_ok=True)
            cache.mkdir(parents=True, exist_ok=True)
            # Probe link support before moving any user files (Windows may
            # require Developer Mode). The temporary link never targets data.
            with tempfile.TemporaryDirectory(dir=data) as temporary:
                probe = Path(temporary) / "link"
                probe.symlink_to(temporary, target_is_directory=True)
                probe.unlink()
            record = _record(source, data, cache)
            _write_journal(journal, record)
        if rollback:
            _rollback(record, source, data, cache, journal)
        else:
            _apply(record, source, data, cache, journal)
    return {"status": "rolled_back" if rollback else "migrated", "data": str(data), "cache": str(cache),
            "message": "Legacy paths are restored." if rollback else
            "Migration completed. Keep the compatibility links in ~/.vireo; you can restart Vireo."}


def run_cli(action: str) -> int:
    """Print a preview/result, including actionable recovery on failure."""
    import sys

    try:
        result = preview() if action == "preview" else migrate(rollback=action == "rollback")
    except (OSError, ValueError) as exc:
        print(f"Storage migration stopped: {exc}", file=sys.stderr)
        if _exists(Path(migration_journal())):
            print("Keep Vireo closed. Use --migrate-storage apply to resume or "
                  "--migrate-storage rollback to restore the previous layout.", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0
