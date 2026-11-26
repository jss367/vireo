"""Catalog snapshots taken before a schema upgrade, and where they are kept.

Before pending migrations run, ``schema.ensure_schema`` writes
``<catalog>.pre-v<N>.bak`` beside the catalog with ``VACUUM INTO``. The
newest ``KEEP_BACKUPS`` schema versions are kept, so a migration bug noticed
a few releases later still has a catalog from before it to restore.

The user may name a backup folder on an external drive or NAS
(``catalog_backup_dir``). The snapshot is still written beside the catalog
first, because the upgrade must not wait on, or fail because of, a slow or
disconnected volume. After launch, ``offload`` copies each local snapshot
into that folder, verifies the copy byte for byte, and only then removes the
local one. A backup folder that does not exist is never created: when a drive
is unplugged its mount point under ``/Volumes`` is gone, and creating the path
would put the backups on the startup disk while looking like they were safe.
Those snapshots stay beside the catalog until the folder is back.

Snapshots for schema versions newer than the running build are never pruned
and do not count toward the kept set: they come from a newer Vireo (the user
restored an older catalog while a later backup remained) and may hold the
only copy of edits made under that schema.
"""

from __future__ import annotations

import contextlib
import glob
import hashlib
import logging
import os
import re
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from file_replace import replace_file

log = logging.getLogger(__name__)

KEEP_BACKUPS = 3

_BACKUP_SUFFIX_RE = re.compile(r"\.pre-v(\d+)\.bak\Z")
_COPY_CHUNK = 8 * 1024 * 1024


@dataclass(frozen=True)
class Backup:
    version: int
    path: str


@dataclass
class OffloadResult:
    """What ``offload`` moved, what it could not, and the bytes it copied."""

    moved: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    bytes_copied: int = 0


def backup_path(db_path: str, target_version: int) -> str:
    """The snapshot of ``db_path`` taken before upgrading to ``target_version``."""
    return f"{db_path}.pre-v{target_version}.bak"


def list_backups(directory: str, catalog_name: str) -> list[Backup]:
    """The ``<catalog_name>.pre-v<N>.bak`` files in ``directory``, newest first."""
    prefix = os.path.join(directory, catalog_name)
    found = []
    for path in glob.glob(glob.escape(prefix) + ".pre-v*.bak"):
        match = _BACKUP_SUFFIX_RE.match(path[len(prefix):])
        if match is not None:
            found.append(Backup(int(match.group(1)), path))
    found.sort(key=lambda backup: (-backup.version, backup.path))
    return found


def snapshot(db_path: str, target_version: int) -> None:
    """Snapshot the catalog beside itself before pending migrations touch it.

    ``VACUUM INTO`` produces a consistent single-file copy even under WAL.
    A snapshot from an earlier (possibly failed) attempt at the same target
    version is kept as-is: it reflects an older, safer state than whatever
    partial progress the failed run left behind. Backup failure (disk full,
    read-only volume) is logged but never blocks startup, because
    per-migration transactions still protect the live file.
    """
    backup = backup_path(db_path, target_version)
    if os.path.exists(backup):
        return
    tmp = backup + ".tmp"
    with contextlib.suppress(OSError):
        os.remove(tmp)
    try:
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            conn.execute("VACUUM INTO ?", (tmp,))
        replace_file(tmp, backup)
        log.info("Backed up database to %s before schema migration", backup)
    except (sqlite3.Error, OSError):
        log.warning(
            "Could not back up %s before schema migration; continuing without "
            "a snapshot", db_path, exc_info=True,
        )
        with contextlib.suppress(OSError):
            os.remove(tmp)


def prune(
    directories: Iterable[str],
    catalog_name: str,
    latest_version: int,
    keep: int = KEEP_BACKUPS,
) -> list[str]:
    """Remove snapshots beyond the newest ``keep`` versions; return what went.

    The kept set is counted across every directory, so a snapshot still
    waiting beside the catalog and the older ones in the backup folder share
    one limit. A version present in two places keeps both copies. A file that
    cannot be removed is logged and left; the next prune tries it again.
    """
    backups = [
        backup
        for directory in dict.fromkeys(directories)
        for backup in list_backups(directory, catalog_name)
        if backup.version <= latest_version
    ]
    kept_versions = sorted({b.version for b in backups}, reverse=True)[:keep]
    removed = []
    for backup in backups:
        if backup.version in kept_versions:
            continue
        try:
            os.remove(backup.path)
        except OSError:
            log.warning("Could not remove old catalog backup %s", backup.path, exc_info=True)
            continue
        log.info("Removed old catalog backup %s", backup.path)
        removed.append(backup.path)
    return removed


def backup_folder_problem(backup_dir: str) -> str | None:
    """Why ``backup_dir`` cannot take backups right now, or None if it can."""
    if not os.path.isabs(backup_dir):
        return f"The catalog backup folder {backup_dir!r} is not an absolute path."
    if not os.path.isdir(backup_dir):
        return (
            f"The catalog backup folder {backup_dir} is not available. If it is "
            "on an external drive or NAS, connect it; Vireo does not create the "
            "folder, so it never writes backups onto the startup disk by mistake."
        )
    if not os.access(backup_dir, os.W_OK):
        return f"Vireo cannot write to the catalog backup folder {backup_dir}."
    return None


def _same_directory(a: str, b: str) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return False


def _copy_verified(
    src: str, dest: str, progress: Callable[[int], None],
) -> int:
    """Copy ``src`` to ``dest``, read ``dest`` back and compare; return its size.

    The copy is written to ``dest + ".tmp"``, flushed to the device, read back
    and hashed, and renamed into place only when the hashes match, so a
    ``.bak`` in the backup folder is always a whole, verified file.
    """
    tmp = dest + ".tmp"
    written = 0
    source_hash = hashlib.sha256()
    try:
        with open(src, "rb") as reader, open(tmp, "wb") as writer:
            while chunk := reader.read(_COPY_CHUNK):
                writer.write(chunk)
                source_hash.update(chunk)
                written += len(chunk)
                progress(written)
            writer.flush()
            os.fsync(writer.fileno())
        if _file_hash(tmp) != source_hash.digest():
            raise OSError(f"the copy of {src} read back different from the original")
        replace_file(tmp, dest)
    finally:
        with contextlib.suppress(OSError):
            os.remove(tmp)
    return written


def offload(
    db_path: str,
    backup_dir: str,
    progress: Callable[[str, int, int], None] | None = None,
) -> OffloadResult:
    """Move the snapshots beside ``db_path`` into ``backup_dir``.

    ``progress(name, bytes_copied, total_bytes)`` is called as each file is
    copied; it may raise to cancel, and the partial copy is removed. A
    snapshot whose name already exists in ``backup_dir`` with different
    contents is left beside the catalog and reported, because the two may
    hold different catalogs.
    """
    result = OffloadResult()
    catalog_dir = os.path.dirname(os.path.abspath(db_path))
    catalog_name = os.path.basename(db_path)
    local = list_backups(catalog_dir, catalog_name)
    if not local or _same_directory(catalog_dir, backup_dir):
        return result
    problem = backup_folder_problem(backup_dir)
    if problem is not None:
        result.errors.append(
            f"{problem} {len(local)} catalog backup(s) stay next to the catalog for now."
        )
        return result
    report = progress or (lambda _name, _done, _total: None)
    for backup in local:
        _move_one(backup.path, backup_dir, report, result)
    return result


def _file_hash(path: str) -> bytes:
    digest = hashlib.sha256()
    with open(path, "rb") as reader:
        while chunk := reader.read(_COPY_CHUNK):
            digest.update(chunk)
    return digest.digest()


def _move_one(
    src: str,
    backup_dir: str,
    report: Callable[[str, int, int], None],
    result: OffloadResult,
) -> None:
    name = os.path.basename(src)
    dest = os.path.join(backup_dir, name)
    try:
        if os.path.exists(dest):
            # A move whose final local delete failed leaves the same file in
            # both places; anything else under that name may be another
            # catalog's snapshot, so only an identical copy lets the local one go.
            if _file_hash(dest) != _file_hash(src):
                result.errors.append(
                    f"{name} is already in {backup_dir} with different contents, "
                    "so the copy next to the catalog was kept. Compare the two "
                    "and delete the one you do not need."
                )
                return
        else:
            total = os.path.getsize(src)

            def progress(done: int) -> None:
                report(name, done, total)

            result.bytes_copied += _copy_verified(src, dest, progress)
        os.remove(src)
    except OSError as exc:
        log.warning("Could not move %s to %s", src, backup_dir, exc_info=True)
        result.errors.append(f"Could not move {name} to {backup_dir}: {exc}")
        return
    log.info("Moved catalog backup %s to %s", src, dest)
    result.moved.append(dest)
