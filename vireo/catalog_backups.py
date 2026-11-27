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
import math
import os
import re
import shutil
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


# Room the backup must leave on the volume beyond its own size: the
# migrations that follow write a WAL on the same volume, and SQLite needs
# scratch pages while it vacuums. The larger of the two bounds applies.
_BACKUP_HEADROOM_FRACTION = 0.05
_BACKUP_HEADROOM_MIN_BYTES = 64 * 1024 * 1024


class MigrationBackupError(RuntimeError):
    """The pre-migration backup could not be written, so the upgrade stopped.

    Raised before any migration touches the catalog, so the live file is
    still at its old schema version and opens again with the build that
    wrote it, or with this build once the backup can be written. The message
    is shown to the user as the startup failure reason, so it names the
    catalog, the backup it tried to write and what to free or fix.
    """

    def __init__(self, db_path: str, backup_path: str, cause: str) -> None:
        self.db_path = db_path
        self.backup_path = backup_path
        self.cause = cause
        super().__init__(
            f"Vireo needs to upgrade the catalog at {db_path} and backs it up "
            f"first, but could not write the backup {backup_path}: {cause}. "
            "The catalog was not changed. Fix this and relaunch Vireo to "
            "finish the upgrade."
        )


def _format_gb(size: int) -> str:
    return f"{size / (1024 ** 3):.1f} GB"


def _format_minimum_to_free(size: int) -> str:
    """Positive amount to free, in a unit that stays non-zero.

    Shown in MB below 1 GB and rounded up, so a near-threshold shortfall
    prints as a usable instruction ("Free at least 5 MB") instead of
    rounding to "0.0 GB".
    """
    if size >= 1024 ** 3:
        gb = math.ceil(size * 10 / (1024 ** 3)) / 10
        return f"{gb:.1f} GB"
    return f"{max(1, math.ceil(size / (1024 ** 2)))} MB"


def _backup_space_needed(db_path: str) -> int:
    """Bytes the backup and the migrations after it need on the volume.

    ``VACUUM INTO`` drops freelist pages, so a catalog that held many deleted
    rows produces a backup much smaller than its raw file length, and it
    folds the current logical state into the output rather than copying the
    WAL separately. The live page count from SQLite bounds the backup;
    estimating from ``os.path.getsize`` would refuse upgrades that fit
    easily. SQLite failing to answer falls back to the raw size —
    conservative but still safe. The existing ``-wal`` file is already part
    of the volume's used bytes and need not be added again: a tiny catalog
    whose WAL is pinned large by another connection would otherwise be
    refused indefinitely. Scratch for migrations that follow the backup is
    covered by ``_BACKUP_HEADROOM_FRACTION`` / ``_BACKUP_HEADROOM_MIN_BYTES``.
    """
    try:
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            page_count = conn.execute("PRAGMA page_count").fetchone()[0]
            freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0]
        size = max(0, page_count - freelist_count) * page_size
    except sqlite3.Error:
        size = os.path.getsize(db_path)
    headroom = max(int(size * _BACKUP_HEADROOM_FRACTION), _BACKUP_HEADROOM_MIN_BYTES)
    return size + headroom


def _check_backup_space(db_path: str, backup: str) -> None:
    folder = os.path.dirname(os.path.abspath(backup))
    needed = _backup_space_needed(db_path)
    try:
        free = shutil.disk_usage(folder).free
    except OSError as e:
        raise MigrationBackupError(
            db_path, backup, f"could not read the free space in {folder} ({e})"
        ) from e
    if free < needed:
        raise MigrationBackupError(
            db_path,
            backup,
            f"the drive holding {folder} has {_format_gb(free)} free and the "
            f"backup needs about {_format_gb(needed)}. Free at least "
            f"{_format_minimum_to_free(needed - free)} on that drive",
        )


def snapshot(db_path: str, target_version: int) -> None:
    """Snapshot the database before pending migrations touch it.

    ``VACUUM INTO`` produces a consistent single-file copy even under WAL.
    A snapshot from an earlier (possibly failed) attempt at the same target
    version is kept as-is — it reflects an older, safer state than whatever
    partial progress the failed run left behind.

    No migration runs without this snapshot. The free space is checked
    first, and a backup that still fails (read-only volume, permission
    denied, the disk filling up during the copy) raises
    ``MigrationBackupError`` before the catalog is opened for migration.
    """
    backup = backup_path(db_path, target_version)
    if os.path.exists(backup):
        return
    tmp = backup + ".tmp"
    with contextlib.suppress(OSError):
        os.remove(tmp)
    _check_backup_space(db_path, backup)
    try:
        with contextlib.closing(sqlite3.connect(db_path)) as conn:
            conn.execute("VACUUM INTO ?", (tmp,))
        replace_file(tmp, backup)
    except (sqlite3.Error, OSError) as e:
        log.error(
            "Could not back up %s before schema migration; not migrating",
            db_path, exc_info=True,
        )
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise MigrationBackupError(db_path, backup, str(e)) from e
    log.info("Backed up database to %s before schema migration", backup)


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
    ``progress`` is also invoked between chunks of the readback so a slow
    verification on an external drive or NAS still observes cancellation.
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
        if _file_hash(tmp, lambda _size: progress(written)) != source_hash.digest():
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


def _file_hash(path: str, checkpoint: Callable[[int], None] | None = None) -> bytes:
    digest = hashlib.sha256()
    with open(path, "rb") as reader:
        while chunk := reader.read(_COPY_CHUNK):
            digest.update(chunk)
            if checkpoint is not None:
                checkpoint(len(chunk))
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
            # Hashing both sides on a slow external drive or NAS must still
            # observe cancellation so a collision does not block shutdown for
            # two full-file reads.
            scanned = 0
            total = os.path.getsize(dest) + os.path.getsize(src)

            def checkpoint(size: int) -> None:
                nonlocal scanned
                scanned = min(scanned + size, total)
                report(name, scanned, total)

            if _file_hash(dest, checkpoint) != _file_hash(src, checkpoint):
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
