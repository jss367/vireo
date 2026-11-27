"""Daily catalog backups, and restoring the catalog from one of them.

A backup is a folder under ``<catalog dir>/backups/`` named for when it was
made. It holds ``catalog.db``, a ``VACUUM INTO`` copy of the catalog that
passed SQLite's ``quick_check``, the workspaces' pipeline and culling result
files (they describe the catalog's photos, so a restored catalog needs the
ones written alongside it), and ``backup.json`` saying what the copy holds.
A backup is written into a ``.partial-`` folder and renamed into place, so a
folder without the prefix is complete.

Restoring takes two steps, because the catalog is open the whole time Vireo
runs and every request, job and cache reads it:

1. ``stage_restore`` (a background job) copies the chosen backup's database to
   ``restore-staged.db`` beside the backups, checks the copy, and writes
   ``restore-pending.json``. That is the slow part, and Vireo keeps running.
2. ``apply_pending_restore`` runs when Vireo next starts, after the
   single-instance lock and before the catalog is opened. It moves the live
   catalog and its result files into a ``before-restore-`` backup and renames
   the staged copy into place: renames only, so startup stays fast. The marker
   records each finished phase, so a launch interrupted part way resumes where
   it stopped instead of repeating a move.

The outcome is written to ``last-restore.json`` for Settings to show. A
backup made by an older Vireo is migrated by the normal startup schema
upgrade; one made by a newer Vireo is refused.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from canonical_schema import SCHEMA_VERSION
from catalog_result_files import RESULT_NAME, result_files
from file_replace import replace_file
from job_contract import progress_event
from job_runner.lineage import JobLineage
from schema import OLDEST_UPGRADABLE_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable

    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict

log = logging.getLogger(__name__)

BACKUP_DIR = "backups"
BACKUP_DB = "catalog.db"
MANIFEST = "backup.json"
PENDING_RESTORE = "restore-pending.json"
LAST_RESTORE = "last-restore.json"
STAGED_DB = "restore-staged.db"
PARTIAL_PREFIX = ".partial-"

# Edit-mask snapshots a recipe's ``local.mask.ref`` points at. A snapshot
# file is content-addressed (``<photo>.<ref>.png``), so copying by filename
# is safe to merge. Kept in sync with ``local_masks``.
EDIT_MASKS_SUBDIR = "edit-masks"
_SNAPSHOT_FILE_RE = re.compile(r"^(\d+)\.([0-9a-f]{12})\.png$")
_LOCAL_MASK_REF_RE = re.compile(r'"ref":\s*"([0-9a-f]{12})"')

BACKUP_JOB = "catalog-backup"
RESTORE_JOB = "catalog-restore-prepare"

# Kinds in the rotating pool, which ``catalog_backup_keep`` bounds. The
# copies set aside by a restore are kept apart, newest BEFORE_RESTORE_KEEP.
ROTATING_KINDS = ("daily", "manual")
BEFORE_RESTORE = "before-restore"
BEFORE_RESTORE_KEEP = 2

# A daily backup is due once the newest rotating copy is this old. Under 24
# hours, so the hourly check does not drift a day later each time.
DAILY_INTERVAL = timedelta(hours=23)
CHECK_INTERVAL_SECONDS = 60 * 60
FIRST_CHECK_DELAY_SECONDS = 120.0

# Room kept free beyond the copy itself, so a backup never takes the space the
# live catalog, its write-ahead log and the generated caches keep writing to.
FREE_SPACE_MARGIN = 5 * 1024 ** 3
COPY_CHUNK = 16 * 1024 ** 2
PROGRESS_SECONDS = 0.5

# Serializes everything that writes under ``backups/``: a backup's prune must
# not delete the copy a restore is staging from, and two copies of a
# multi-gigabyte catalog at once would only race each other for the disk.
_LOCK = threading.Lock()


class BackupError(RuntimeError):
    """A backup or restore step that stopped; the message is for the user."""


class BackupBusy(BackupError):
    """Another backup or restore is in progress."""


@dataclass(frozen=True)
class BackupHooks:
    """How a running backup reports progress and learns it was cancelled."""

    cancelled: Callable[[], bool]
    progress: Callable[[str, int, int], None]


def backups_dir(db_path: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), BACKUP_DIR)


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(when: datetime) -> str:
    return when.astimezone(UTC).isoformat(timespec="seconds")


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _read_json(path: str) -> JsonDict | None:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        log.warning("Could not read %s", path, exc_info=True)
        return None
    return data if isinstance(data, dict) else None


def _write_json(path: str, data: JsonDict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    replace_file(tmp, path)


def _remove(path: str) -> None:
    with contextlib.suppress(FileNotFoundError):
        os.remove(path)


def _unique_name(root: str, prefix: str, when: datetime) -> str:
    base = f"{prefix}-{when.astimezone():%Y%m%d-%H%M%S}"
    name, suffix = base, 2
    while os.path.exists(os.path.join(root, name)) or os.path.exists(
        os.path.join(root, PARTIAL_PREFIX + name)
    ):
        name, suffix = f"{base}-{suffix}", suffix + 1
    return name


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _folder_size(path: str) -> int:
    total = 0
    with contextlib.suppress(OSError):
        for dirpath, _dirnames, filenames in os.walk(path):
            for name in filenames:
                with contextlib.suppress(OSError):
                    total += os.lstat(os.path.join(dirpath, name)).st_size
    return total


def _require_free_space(root: str, needed: int, what: str) -> None:
    free = shutil.disk_usage(root).free
    if free < needed + FREE_SPACE_MARGIN:
        raise BackupError(
            f"Not enough free disk space for {what}: it needs about "
            f"{_gb(needed + FREE_SPACE_MARGIN)} (including {_gb(FREE_SPACE_MARGIN)} kept "
            f"free for Vireo's own files), and {_gb(free)} is free in {root}."
        )


def _gb(size: int) -> str:
    return f"{size / 1024 ** 3:.1f} GB"


def _size(size: int) -> str:
    for unit, scale in (("GB", 1024 ** 3), ("MB", 1024 ** 2), ("KB", 1024)):
        if size >= scale:
            return f"{size / scale:.1f} {unit}"
    return f"{size} bytes"


def _stop_if_cancelled(hooks: BackupHooks, what: str = "Catalog backup") -> None:
    if hooks.cancelled():
        raise BackupError(f"{what} cancelled")


def _progress_handler(hooks: BackupHooks, report: Callable[[], None] | None = None) -> Callable[[], int]:
    """A SQLite progress handler that stops the statement once cancelled."""
    last = [0.0]

    def handler() -> int:
        if hooks.cancelled():
            return 1
        now = time.monotonic()
        if report is not None and now - last[0] >= PROGRESS_SECONDS:
            last[0] = now
            report()
        return 0

    return handler


def _vacuum_into(db_path: str, target: str, hooks: BackupHooks) -> None:
    """Write a consistent copy of the catalog to ``target``.

    ``VACUUM INTO`` reads one snapshot, so writers keep going while it runs
    and the copy holds no half-finished transaction. Progress is the copy's
    size against the live file's, which overstates the work left when the
    catalog has free pages the copy leaves out.
    """
    total = _file_size(db_path)

    def report() -> None:
        hooks.progress("Copying the catalog", min(_file_size(target), total), total)

    with contextlib.closing(sqlite3.connect(db_path, timeout=60)) as conn:
        conn.set_progress_handler(_progress_handler(hooks, report), 20000)
        try:
            conn.execute("VACUUM INTO ?", (target,))
        except sqlite3.OperationalError as exc:
            if hooks.cancelled():
                raise BackupError("Catalog backup cancelled") from exc
            raise BackupError(f"SQLite could not copy the catalog: {exc}") from exc


def _count(conn: sqlite3.Connection, table: str) -> int | None:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    except sqlite3.OperationalError:
        return None
    return int(row[0])


def _describe_db(path: str, hooks: BackupHooks | None = None) -> JsonDict:
    """The schema version and row counts of a catalog file.

    With ``hooks``, also runs ``PRAGMA quick_check`` and raises BackupError
    when it finds damage: a copy that fails it is not worth keeping.
    """
    with contextlib.closing(sqlite3.connect(path, timeout=60)) as conn:
        if hooks is not None:
            hooks.progress("Checking the copy", 0, 0)
            conn.set_progress_handler(_progress_handler(hooks), 50000)
            try:
                rows = conn.execute("PRAGMA quick_check").fetchall()
            except sqlite3.OperationalError as exc:
                if hooks.cancelled():
                    raise BackupError("Catalog backup cancelled") from exc
                raise BackupError(f"SQLite could not check the copy: {exc}") from exc
            if [tuple(row) for row in rows] != [("ok",)]:
                problems = "; ".join(str(row[0]) for row in rows[:3])
                raise BackupError(f"The copy failed SQLite's integrity check: {problems}")
            conn.set_progress_handler(None, 0)
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        return {
            "schema_version": version,
            "photos": _count(conn, "photos"),
            "workspaces": _count(conn, "workspaces"),
        }


def _copy_results(catalog_dir: str, dest: str) -> int:
    """Copy the workspaces' result files into ``dest``; return how many."""
    copied = 0
    for path, _kind, _workspace_id in result_files(catalog_dir):
        try:
            shutil.copy2(path, os.path.join(dest, os.path.basename(path)))
        except FileNotFoundError:
            continue  # a workspace deleted while the backup ran
        copied += 1
    return copied


def _remove_partials(root: str) -> None:
    with contextlib.suppress(FileNotFoundError), os.scandir(root) as entries:
        partials = [e.path for e in entries if e.name.startswith(PARTIAL_PREFIX)]
    for path in partials:
        shutil.rmtree(path, ignore_errors=True)


def _referenced_mask_refs(conn: sqlite3.Connection) -> set[str]:
    """Edit-mask refs any recipe or edit-history row in ``conn`` points at.

    Mirrors the three reads behind ``local_masks._referenced_refs`` so the
    backup can enumerate refs from its ``VACUUM INTO`` copy without opening
    a full ``Database``. A missing table (older schema) contributes no refs.
    """
    refs: set[str] = set()
    queries = (
        "SELECT recipe_json FROM photo_edit_recipes WHERE recipe_json LIKE '%\"local\"%'",
        "SELECT value_json FROM photo_versions WHERE value_json LIKE '%\"local\"%'",
        "SELECT old_value FROM edit_history_items WHERE old_value LIKE '%\"local\"%'",
        "SELECT new_value FROM edit_history_items WHERE new_value LIKE '%\"local\"%'",
    )
    for query in queries:
        try:
            rows = conn.execute(query).fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            refs.update(_LOCAL_MASK_REF_RE.findall(row[0] or ""))
    return refs


def _mask_refs_from_db_file(db_path: str) -> set[str]:
    try:
        with contextlib.closing(sqlite3.connect(db_path, timeout=60)) as conn:
            return _referenced_mask_refs(conn)
    except sqlite3.Error:
        log.warning("Could not read edit-mask refs from %s", db_path, exc_info=True)
        return set()


def _copy_snapshot_atomically(src: str, dest: str, name: str) -> bool:
    """Copy ``src`` to ``dest`` via a ``.partial`` sibling and ``replace_file``.

    An interrupted copy leaves the tmp (never the final name), so a later
    pass does not mistake a truncated file for a complete one. Returns
    True on success and False when the source disappeared between the
    directory listing and the copy (a ``gc_edit_masks`` sweep ran against
    the live catalog in between); every other failure propagates so the
    caller can abort rather than publish an incomplete backup or restore.
    """
    tmp = dest + ".partial"
    try:
        shutil.copy2(src, tmp)
    except FileNotFoundError:
        log.warning("Edit-mask snapshot %s disappeared before it could be copied", name)
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp)
        return False
    replace_file(tmp, dest)
    return True


def _referenced_snapshot_bytes(media_dir: str, refs: set[str]) -> int:
    """Total on-disk bytes of every referenced ``<photo>.<ref>.png`` snapshot.

    Used by the backup preflight to reserve space for the snapshots it will
    copy, so a live catalog's edit-masks cannot consume the ``FREE_SPACE_MARGIN``
    the preflight keeps for its own writes.
    """
    if not refs or not media_dir:
        return 0
    source = os.path.join(media_dir, EDIT_MASKS_SUBDIR)
    try:
        entries = os.listdir(source)
    except FileNotFoundError:
        return 0
    total = 0
    for name in entries:
        match = _SNAPSHOT_FILE_RE.match(name)
        if match is None or match.group(2) not in refs:
            continue
        total += _file_size(os.path.join(source, name))
    return total


def _copy_referenced_snapshots(media_dir: str, dest_dir: str, refs: set[str]) -> int:
    """Copy every ``<photo>.<ref>.png`` snapshot whose ref is referenced.

    A snapshot that disappeared between the directory listing and the copy
    is skipped (it is already lost on disk and nothing here can bring it
    back). Every other error propagates so the caller aborts the backup
    rather than publishing a copy whose recipes point at snapshots the
    backup does not carry.
    """
    if not refs or not media_dir:
        return 0
    source = os.path.join(media_dir, EDIT_MASKS_SUBDIR)
    try:
        entries = os.listdir(source)
    except FileNotFoundError:
        return 0
    copied = 0
    os.makedirs(dest_dir, exist_ok=True)
    for name in entries:
        match = _SNAPSHOT_FILE_RE.match(name)
        if match is None or match.group(2) not in refs:
            continue
        if _copy_snapshot_atomically(
            os.path.join(source, name), os.path.join(dest_dir, name), name,
        ):
            copied += 1
    return copied


def _restore_edit_mask_snapshots(source_dir: str, media_dir: str) -> int:
    """Copy snapshot files from ``source_dir`` into the live edit-masks folder.

    Existing files are left alone: a snapshot's filename is the hash of
    its contents, so a name match is a bytes match. Copies run through a
    tmp file and ``replace_file`` so a launch interrupted mid-copy leaves
    no truncated file at the final name that a resumed restore would
    then treat as complete. A copy failure propagates so ``_apply_phases``
    rolls the restore back instead of swapping in a catalog whose recipes
    reference absent snapshots.
    """
    if not media_dir:
        return 0
    try:
        entries = os.listdir(source_dir)
    except FileNotFoundError:
        return 0
    target = os.path.join(media_dir, EDIT_MASKS_SUBDIR)
    os.makedirs(target, exist_ok=True)
    added = 0
    for name in entries:
        if _SNAPSHOT_FILE_RE.match(name) is None:
            continue
        dest = os.path.join(target, name)
        if os.path.exists(dest):
            continue
        if _copy_snapshot_atomically(os.path.join(source_dir, name), dest, name):
            added += 1
    return added


def _restorable(version: object) -> tuple[bool, str | None]:
    if not isinstance(version, int):
        return False, "The backup does not record its catalog version."
    if version > SCHEMA_VERSION:
        return False, "A newer version of Vireo made this backup. Update Vireo to restore it."
    if version < OLDEST_UPGRADABLE_VERSION:
        return False, "This backup is older than this version of Vireo can upgrade."
    return True, None


def list_backups(db_path: str) -> list[JsonDict]:
    """Every complete backup, newest first."""
    root = backups_dir(db_path)
    found: list[JsonDict] = []
    try:
        with os.scandir(root) as entries:
            folders = [e for e in entries if e.is_dir(follow_symlinks=False)
                       and not e.name.startswith(".")]
    except FileNotFoundError:
        return []
    for entry in folders:
        manifest = _read_json(os.path.join(entry.path, MANIFEST))
        if manifest is None or not os.path.isfile(os.path.join(entry.path, BACKUP_DB)):
            continue
        restorable, reason = _restorable(manifest.get("schema_version"))
        found.append({
            **manifest,
            "name": entry.name,
            "bytes": _folder_size(entry.path),
            "restorable": restorable,
            "not_restorable_reason": reason,
        })
    found.sort(key=lambda b: (str(b.get("created_at") or ""), b["name"]), reverse=True)
    return found


def _prune(db_path: str, kinds: tuple[str, ...], keep: int, protect: str | None) -> list[str]:
    """Delete the oldest backups of ``kinds`` beyond the newest ``keep``."""
    root = backups_dir(db_path)
    pool = [b for b in list_backups(db_path) if b.get("kind") in kinds]
    removed = []
    for backup in pool[keep:]:
        if backup["name"] == protect:
            continue
        shutil.rmtree(os.path.join(root, backup["name"]), ignore_errors=True)
        removed.append(backup["name"])
    return removed


def pending_restore(db_path: str) -> JsonDict | None:
    return _read_json(os.path.join(backups_dir(db_path), PENDING_RESTORE))


def last_restore(db_path: str) -> JsonDict | None:
    return _read_json(os.path.join(backups_dir(db_path), LAST_RESTORE))


def _pending_backup_name(db_path: str) -> str | None:
    marker = pending_restore(db_path)
    name = marker.get("backup") if marker else None
    return name if isinstance(name, str) else None


def is_due(db_path: str, now: datetime | None = None) -> bool:
    """Whether the newest rotating backup is old enough for the next one."""
    now = now or _now()
    for backup in list_backups(db_path):
        if backup.get("kind") in ROTATING_KINDS:
            created = _parse_iso(backup.get("created_at"))
            return created is None or now - created >= DAILY_INTERVAL
    return True


def create_backup(
    db_path: str, *, kind: str, keep: int, hooks: BackupHooks,
    media_dir: str | None = None, now: datetime | None = None,
) -> JsonDict:
    """Back up the catalog, then delete rotating copies beyond ``keep``.

    At least one rotating copy is kept whatever ``keep`` says, so a backup
    made while the daily backup is off is not deleted as soon as it lands.
    ``media_dir`` is the root under which ``edit-masks/<photo>.<ref>.png``
    snapshots live; the backup keeps a copy of every snapshot its catalog
    still references, so a later ``gc_edit_masks`` sweep cannot leave a
    restored recipe rendering without its local adjustment. When omitted,
    defaults to the catalog's directory (the default-install layout).
    """
    if not _LOCK.acquire(blocking=False):
        raise BackupBusy("Another catalog backup or restore is in progress.")
    try:
        root = backups_dir(db_path)
        os.makedirs(root, exist_ok=True)
        _remove_partials(root)
        catalog_dir = os.path.dirname(os.path.abspath(db_path))
        masks_root = media_dir or catalog_dir
        results_bytes = sum(_file_size(path) for path, _k, _w in result_files(catalog_dir))
        mask_bytes = _referenced_snapshot_bytes(masks_root, _mask_refs_from_db_file(db_path))
        _require_free_space(
            root, _file_size(db_path) + results_bytes + mask_bytes, "a catalog backup",
        )
        when = now or _now()
        name = _unique_name(root, "catalog", when)
        partial = os.path.join(root, PARTIAL_PREFIX + name)
        os.mkdir(partial)
        published = False
        try:
            _stop_if_cancelled(hooks)
            _vacuum_into(db_path, os.path.join(partial, BACKUP_DB), hooks)
            _stop_if_cancelled(hooks)
            hooks.progress("Copying workspace results", 0, 0)
            results = _copy_results(catalog_dir, partial)
            facts = _describe_db(os.path.join(partial, BACKUP_DB), hooks)
            _stop_if_cancelled(hooks)
            hooks.progress("Copying edit-mask snapshots", 0, 0)
            refs = _mask_refs_from_db_file(os.path.join(partial, BACKUP_DB))
            snapshots = _copy_referenced_snapshots(
                masks_root, os.path.join(partial, EDIT_MASKS_SUBDIR), refs,
            )
            _stop_if_cancelled(hooks)
            manifest: JsonDict = {"kind": kind, "created_at": _iso(when), **facts,
                                  "result_files": results, "edit_masks": snapshots}
            _write_json(os.path.join(partial, MANIFEST), manifest)
            replace_file(partial, os.path.join(root, name))
            published = True
        finally:
            if not published:
                shutil.rmtree(partial, ignore_errors=True)
        hooks.progress("Removing older backups", 0, 0)
        removed = _prune(db_path, ROTATING_KINDS, max(keep, 1), _pending_backup_name(db_path))
    finally:
        _LOCK.release()
    size = _folder_size(os.path.join(root, name))
    summary = f"Backed up the catalog ({_size(size)})"
    if removed:
        summary += f"; removed {len(removed)} older backup{'s' if len(removed) != 1 else ''}"
    return {
        "backup": name, "bytes": size, "photos": facts.get("photos"),
        "removed_backups": removed, "summary": summary,
    }


def _copy_file(src: str, dest: str, hooks: BackupHooks) -> None:
    total = _file_size(src)
    copied, last = 0, 0.0
    with open(src, "rb") as reader, open(dest, "wb") as writer:
        while chunk := reader.read(COPY_CHUNK):
            _stop_if_cancelled(hooks, "Restore preparation")
            writer.write(chunk)
            copied += len(chunk)
            if time.monotonic() - last >= PROGRESS_SECONDS:
                last = time.monotonic()
                hooks.progress("Copying the backup", copied, total)
        writer.flush()
        os.fsync(writer.fileno())


def stage_restore(db_path: str, name: str, hooks: BackupHooks, now: datetime | None = None) -> JsonDict:
    """Prepare ``name`` to replace the catalog at the next launch."""
    backup = next((b for b in list_backups(db_path) if b["name"] == name), None)
    if backup is None:
        raise BackupError(f"There is no backup named {name!r}.")
    if not backup["restorable"]:
        raise BackupError(backup["not_restorable_reason"])
    if not _LOCK.acquire(blocking=False):
        raise BackupBusy("Another catalog backup or restore is in progress.")
    try:
        root = backups_dir(db_path)
        marker_path, staged = os.path.join(root, PENDING_RESTORE), os.path.join(root, STAGED_DB)
        _remove(marker_path)
        _remove(staged)
        source = os.path.join(root, name, BACKUP_DB)
        _require_free_space(root, _file_size(source), "the restore")
        partial = staged + ".partial"
        try:
            _copy_file(source, partial, hooks)
            _describe_db(partial, hooks)
            _stop_if_cancelled(hooks, "Restore preparation")
            replace_file(partial, staged)
        finally:
            _remove(partial)
        _write_json(marker_path, {
            "backup": name, "backup_created_at": backup.get("created_at"),
            "staged_at": _iso(now or _now()), "phase": "staged",
        })
    finally:
        _LOCK.release()
    return {
        "backup": name,
        "summary": "The backup is ready to restore. Quit and reopen Vireo to finish restoring it.",
    }


def cancel_restore(db_path: str) -> bool:
    """Drop a staged restore; return whether one was waiting."""
    if not _LOCK.acquire(blocking=False):
        raise BackupBusy("A backup or restore is being prepared. Cancel that job first.")
    try:
        root = backups_dir(db_path)
        marker = os.path.join(root, PENDING_RESTORE)
        waiting = os.path.exists(marker)
        _remove(marker)
        _remove(os.path.join(root, STAGED_DB))
        return waiting
    finally:
        _LOCK.release()


def status(db_path: str, keep: int) -> JsonDict:
    """What Settings shows: the backups, the schedule and any restore."""
    root = backups_dir(db_path)
    backups = list_backups(db_path)
    newest = next((b for b in backups if b.get("kind") in ROTATING_KINDS), None)
    next_due = None
    if keep > 0:
        created = _parse_iso(newest.get("created_at")) if newest else None
        next_due = _iso(created + DAILY_INTERVAL) if created else None
    try:
        free = shutil.disk_usage(root if os.path.isdir(root) else os.path.dirname(root)).free
    except OSError:
        free = None
    return {
        "directory": root,
        "keep": keep,
        "daily_enabled": keep > 0,
        "backups": backups,
        "total_bytes": sum(b["bytes"] for b in backups),
        "free_bytes": free,
        "last_backup_at": newest.get("created_at") if newest else None,
        "next_due_at": next_due,
        "pending_restore": pending_restore(db_path),
        "last_restore": last_restore(db_path),
    }


# --- Applying a staged restore at startup ---------------------------------


def _checkpoint_live(db_path: str) -> None:
    """Fold the WAL into the catalog file so the file alone is the catalog."""
    with contextlib.closing(sqlite3.connect(db_path, timeout=60)) as conn:
        busy, _log_frames, _done = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if busy or _file_size(db_path + "-wal") > 0:
        raise BackupError(
            "The catalog's write-ahead log could not be folded into it, so the "
            "restore did not replace it."
        )
    _remove(db_path + "-wal")
    _remove(db_path + "-shm")


def _set_aside_live_catalog(
    db_path: str, root: str, marker: JsonDict, media_dir: str,
) -> JsonDict:
    """Move the live catalog and its result files into a before-restore backup.

    Each move is skipped when its source is already gone, so a launch that
    stopped part way through finishes the remaining moves. Edit-mask
    snapshots the set-aside catalog still references are copied into the
    before-restore folder: that preserves them for an undo-by-restoring
    even after a later ``gc_edit_masks`` sweep against the restored catalog.
    """
    before = marker.get("before_restore")
    if not isinstance(before, str):
        before = _unique_name(root, BEFORE_RESTORE, _now())
        marker = {**marker, "before_restore": before}
        _write_json(os.path.join(root, PENDING_RESTORE), marker)
    before_dir = os.path.join(root, before)
    os.makedirs(before_dir, exist_ok=True)
    kept_db = os.path.join(before_dir, BACKUP_DB)
    if os.path.exists(db_path):
        _checkpoint_live(db_path)
        replace_file(db_path, kept_db)
    catalog_dir = os.path.dirname(os.path.abspath(db_path))
    for path, _kind, _workspace_id in result_files(catalog_dir):
        replace_file(path, os.path.join(before_dir, os.path.basename(path)))
    facts: JsonDict = _describe_db(kept_db) if os.path.exists(kept_db) else {}
    snapshots = _copy_referenced_snapshots(
        media_dir, os.path.join(before_dir, EDIT_MASKS_SUBDIR),
        _mask_refs_from_db_file(kept_db) if os.path.exists(kept_db) else set(),
    )
    _write_json(os.path.join(before_dir, MANIFEST), {
        "kind": BEFORE_RESTORE, "created_at": _iso(_now()), **facts,
        "replaced_by": marker.get("backup"), "edit_masks": snapshots,
    })
    marker = {**marker, "phase": "moved_live"}
    _write_json(os.path.join(root, PENDING_RESTORE), marker)
    return marker


def _restore_result_files(source_dir: str, catalog_dir: str) -> None:
    with contextlib.suppress(FileNotFoundError), os.scandir(source_dir) as entries:
        names = [e.name for e in entries if RESULT_NAME.fullmatch(e.name)]
    for name in names:
        tmp = os.path.join(catalog_dir, name + ".restore-tmp")
        shutil.copy2(os.path.join(source_dir, name), tmp)
        replace_file(tmp, os.path.join(catalog_dir, name))


def _put_backup_in_place(
    db_path: str, root: str, marker: JsonDict, media_dir: str,
) -> JsonDict:
    catalog_dir = os.path.dirname(os.path.abspath(db_path))
    backup_dir = os.path.join(root, str(marker["backup"]))
    _restore_result_files(backup_dir, catalog_dir)
    _restore_edit_mask_snapshots(
        os.path.join(backup_dir, EDIT_MASKS_SUBDIR), media_dir,
    )
    staged = os.path.join(root, STAGED_DB)
    if os.path.exists(staged):
        _remove(db_path + "-wal")
        _remove(db_path + "-shm")
        replace_file(staged, db_path)
    elif not os.path.exists(db_path):
        raise BackupError("The prepared copy of the backup disappeared before it was put in place.")
    marker = {**marker, "phase": "swapped"}
    _write_json(os.path.join(root, PENDING_RESTORE), marker)
    return marker


def _put_live_catalog_back(db_path: str, root: str, marker: JsonDict) -> None:
    """Undo ``_set_aside_live_catalog``, finished or not.

    Result files in the catalog folder are removed only once the live ones
    have all been set aside (phase ``moved_live``): before that, a file there
    is a live one the move had not reached yet.
    """
    before = marker.get("before_restore")
    if not isinstance(before, str):
        return  # nothing was moved
    before_dir = os.path.join(root, before)
    catalog_dir = os.path.dirname(os.path.abspath(db_path))
    if marker.get("phase") == "moved_live":
        for path, _kind, _workspace_id in result_files(catalog_dir):
            _remove(path)
    try:
        with os.scandir(before_dir) as entries:
            names = [e.name for e in entries if RESULT_NAME.fullmatch(e.name)]
    except FileNotFoundError:
        return
    for name in names:
        replace_file(os.path.join(before_dir, name), os.path.join(catalog_dir, name))
    kept_db = os.path.join(before_dir, BACKUP_DB)
    if os.path.exists(kept_db):
        replace_file(kept_db, db_path)
    shutil.rmtree(before_dir, ignore_errors=True)


def _apply_phases(
    db_path: str, root: str, marker: JsonDict, media_dir: str,
) -> JsonDict:
    if marker.get("phase") == "staged":
        if not os.path.exists(os.path.join(root, STAGED_DB)):
            raise BackupError("The prepared copy of the backup is missing, so nothing was changed.")
        try:
            marker = _set_aside_live_catalog(db_path, root, marker, media_dir)
        except (OSError, sqlite3.Error, BackupError) as exc:
            # The marker on disk names the before-restore folder once one
            # was chosen; the local copy does not.
            _put_live_catalog_back(db_path, root, pending_restore(db_path) or marker)
            raise BackupError(f"Could not set the current catalog aside: {exc}") from exc
    if marker.get("phase") == "moved_live":
        try:
            marker = _put_backup_in_place(db_path, root, marker, media_dir)
        except (OSError, BackupError) as exc:
            # Raises when the live catalog cannot be put back either: Vireo
            # must not start and create an empty catalog in its place.
            _put_live_catalog_back(db_path, root, marker)
            raise BackupError(f"Could not put the backup in place: {exc}") from exc
    if marker.get("phase") != "swapped":
        raise BackupError(f"Unknown restore phase {marker.get('phase')!r}.")
    return marker


def apply_pending_restore(db_path: str, media_dir: str | None = None) -> JsonDict | None:
    """Finish a staged restore before the catalog is opened.

    Returns the outcome written to ``last-restore.json``, or None when no
    restore was waiting. A failure that leaves the live catalog in place is
    recorded and Vireo starts on it; one that cannot put it back raises.
    ``media_dir`` is the root under which ``edit-masks/`` lives; the
    restore copies the backup's referenced snapshots back into it and
    preserves the pre-restore ones in the before-restore folder so undo
    stays complete. Defaults to the catalog's directory for the default
    install layout.
    """
    root = backups_dir(db_path)
    marker = pending_restore(db_path)
    if marker is None:
        return None
    if not isinstance(marker.get("backup"), str):
        _remove(os.path.join(root, PENDING_RESTORE))
        return None
    masks_root = media_dir or os.path.dirname(os.path.abspath(db_path))
    outcome: JsonDict = {
        "backup": marker["backup"], "backup_created_at": marker.get("backup_created_at"),
        "finished_at": None, "ok": False, "error": None, "before_restore": None,
    }
    try:
        marker = _apply_phases(db_path, root, marker, masks_root)
        outcome.update(ok=True, before_restore=marker.get("before_restore"))
        log.info("Restored the catalog from backup %s; the previous catalog is in %s",
                 marker["backup"], marker.get("before_restore"))
        _prune(db_path, (BEFORE_RESTORE,), BEFORE_RESTORE_KEEP, None)
    except BackupError as exc:
        outcome["error"] = str(exc)
        log.error("Catalog restore from %s failed: %s", marker["backup"], exc)
    outcome["finished_at"] = _iso(_now())
    _write_json(os.path.join(root, LAST_RESTORE), outcome)
    _remove(os.path.join(root, PENDING_RESTORE))
    _remove(os.path.join(root, STAGED_DB))
    return outcome


# --- Jobs and the daily schedule ------------------------------------------


def keep_setting() -> int:
    import config as cfg

    value = cfg.load().get("catalog_backup_keep", 0)
    return value if isinstance(value, int) and value > 0 else 0


class CatalogBackups:
    """The per-app daily schedule and the jobs that back up and restore.

    Vireo checks hourly and starts a backup when the newest rotating copy is
    older than ``DAILY_INTERVAL``, so a Vireo that is closed overnight still
    backs up soon after it next opens.
    """

    def __init__(
        self, db_path: str, get_runner: Callable[[], JobRunner], media_dir: str | None = None,
    ) -> None:
        self.db_path = db_path
        self.media_dir = media_dir or os.path.dirname(os.path.abspath(db_path))
        self._get_runner = get_runner
        self._timer: threading.Timer | None = None
        self._stopped = threading.Event()

    def schedule(self, delay: float = FIRST_CHECK_DELAY_SECONDS) -> None:
        if self._stopped.is_set():
            return
        timer = threading.Timer(delay, self._check)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def stop(self) -> None:
        self._stopped.set()
        if self._timer is not None:
            self._timer.cancel()

    def _check(self) -> None:
        if self._stopped.is_set():
            return
        try:
            if keep_setting() > 0 and is_due(self.db_path):
                self.start_backup(kind="daily", lineage=JobLineage.system(
                    "Daily catalog backup, so a bug or a mistake can be undone "
                    "by restoring yesterday's catalog"
                ))
        except (RuntimeError, OSError):
            log.exception("Could not start the daily catalog backup")
        self.schedule(CHECK_INTERVAL_SECONDS)

    def _hooks(self, job: JobDict) -> BackupHooks:
        runner = self._get_runner()

        def progress(phase: str, current: int, total: int) -> None:
            runner.push_event(job["id"], "progress", progress_event(phase, current, total))

        return BackupHooks(cancelled=lambda: runner.is_cancelled(job["id"]), progress=progress)

    def start_backup(self, *, kind: str, lineage: JobLineage | None = None) -> tuple[str, bool]:
        """Start a backup job, or join the one running; return (id, joined)."""
        def work(job: JobDict) -> JsonDict:
            return create_backup(
                self.db_path, kind=kind, keep=keep_setting(), hooks=self._hooks(job),
                media_dir=self.media_dir,
            )

        job_id, joined, _snapshot = self._get_runner().start_singleton(
            BACKUP_JOB, work, singleton_key=BACKUP_JOB, workspace_id=None,
            config={"kind": kind}, lineage=lineage,
        )
        return job_id, joined

    def start_restore(self, name: str) -> str:
        def work(job: JobDict) -> JsonDict:
            return stage_restore(self.db_path, name, self._hooks(job))

        job_id, _joined, _snapshot = self._get_runner().start_singleton(
            RESTORE_JOB, work, singleton_key=RESTORE_JOB, workspace_id=None,
            config={"backup": name},
        )
        return job_id

    def active_job(self) -> JsonDict | None:
        for job in self._get_runner().list_jobs():
            if job.get("type") in (BACKUP_JOB, RESTORE_JOB) and job.get("status") in ("running", "queued"):
                return {"id": job["id"], "type": job["type"], "config": job.get("config") or {}}
        return None

    def status(self) -> JsonDict:
        return {**status(self.db_path, keep_setting()), "active_job": self.active_job()}
