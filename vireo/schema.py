"""One-time database schema initialization and ordered migrations.

``canonical_schema.py`` (run by ``Database._create_tables``) builds the
schema; ``MIGRATIONS`` holds the numbered changes made after
``BASELINE_VERSION``. This module is the startup boundary: web requests open
an initialized database and never perform schema work.
"""

from __future__ import annotations

import contextlib
import glob
import logging
import os
import re
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass

from db import Database, IncompatibleDatabaseError
from file_replace import replace_file

log = logging.getLogger(__name__)

_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[sqlite3.Connection], None]
    validate: Callable[[sqlite3.Connection], None] | None = None


# The version every catalog is stamped to before registry migrations run.
# Migrations 5-12 were retired once every catalog had applied them, and
# ``create_tables`` now creates their end state directly. Later changes are
# ``Migration``s numbered from ``BASELINE_VERSION + 1``; ``create_tables``
# builds their end state too and stamps a fresh database with the newest one
# (``canonical_schema.SCHEMA_VERSION``), so each migration must be a no-op on
# a catalog that already has its change.
BASELINE_VERSION = 12

# The oldest stamped catalog ``create_tables`` alone can bring to the
# baseline: migration 12 only created objects ``create_tables`` already
# creates, so a version-11 catalog needs nothing else. Anything older relied
# on a retired migration (column adds, the MegaDetector alias merge, the
# grouping-history split) and is refused rather than stamped as upgraded.
OLDEST_UPGRADABLE_VERSION = 11

def _add_companion_file_mtime(conn):
    """Record each paired JPEG's mtime beside its import identity.

    A rescan compares a companion JPEG's size and mtime with this row to
    see that the RAW's companion is unchanged, the way an incremental scan
    checks a photo's own row. Existing rows start NULL: the next scan
    hashes those companions once and fills it in.
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(companion_identities)")}
    if "file_mtime" not in columns:
        conn.execute("ALTER TABLE companion_identities ADD COLUMN file_mtime REAL")


def _validate_companion_file_mtime(conn):
    columns = {row[1] for row in conn.execute("PRAGMA table_info(companion_identities)")}
    if "file_mtime" not in columns:
        raise RuntimeError("companion_identities.file_mtime is missing")


def _add_companion_sync_state(conn):
    """Distinguish recovered hashes from identities synchronized by pairing."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(companion_identities)")}
    if "needs_sync" not in columns:
        conn.execute(
            "ALTER TABLE companion_identities"
            " ADD COLUMN needs_sync INTEGER NOT NULL DEFAULT 0"
        )


def _validate_companion_sync_state(conn):
    columns = {row[1] for row in conn.execute("PRAGMA table_info(companion_identities)")}
    if "needs_sync" not in columns:
        raise RuntimeError("companion_identities.needs_sync is missing")


def _add_paired_preview_cache(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS paired_preview_cache (
        filename TEXT PRIMARY KEY,
        photo_id INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
        bytes INTEGER NOT NULL,
        last_access_at REAL NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_paired_preview_photo "
                 "ON paired_preview_cache(photo_id)")


MIGRATIONS = (
    Migration(
        13, "companion_identities_file_mtime",
        _add_companion_file_mtime, _validate_companion_file_mtime,
    ),
    Migration(
        14, "companion_identities_sync_state",
        _add_companion_sync_state, _validate_companion_sync_state,
    ),
    Migration(15, "durable_paired_preview_cache", _add_paired_preview_cache),
)


def _latest_version():
    return MIGRATIONS[-1].version if MIGRATIONS else BASELINE_VERSION


def _apply_pending(conn):
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    latest = _latest_version()
    if current > latest:
        raise RuntimeError(f"database schema version {current} is newer than supported {latest}")
    if current < BASELINE_VERSION:
        conn.execute(f"PRAGMA user_version = {BASELINE_VERSION}")
        current = BASELINE_VERSION

    for migration in MIGRATIONS:
        if migration.version <= current:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            migration.apply(conn)
            if migration.validate is not None:
                migration.validate(conn)
            conn.execute(f"PRAGMA user_version = {migration.version}")
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
        current = migration.version


def _existing_user_version(db_path):
    """``PRAGMA user_version`` of an existing database file, or None for a
    fresh install (missing/empty file)."""
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return None
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        return conn.execute("PRAGMA user_version").fetchone()[0]


def _has_tables(db_path):
    """Whether ``db_path`` already holds any table (a populated catalog,
    not a file only just created)."""
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone() is not None


def _unconverted_legacy_sync_only_folder_grants(db_path):
    """Rows left in the legacy ``workspace_sync_only_folders`` table.

    The retired migration best-effort rekeyed each folder-grant to one or
    more ``workspace_sync_only_photos`` rows, matching the granted folder
    against each pending-change photo's current folder or its
    ``last_move_source_folder_path``. A row that no pending photo could be
    matched to -- for instance, after a photo was moved out of the granted
    folder and its ``last_move_source_folder_path`` was later cleared --
    stayed in the legacy table waiting for a future match. With the
    read-time fallback and the migration both removed, those grants are
    silently inert; refuse rather than lose them.
    """
    if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
        return 0
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        legacy = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='workspace_sync_only_folders'"
        ).fetchone()
        if legacy is None:
            return 0
        return conn.execute(
            "SELECT COUNT(*) FROM workspace_sync_only_folders"
        ).fetchone()[0]


def _backup_path(db_path, target_version):
    return f"{db_path}.pre-v{target_version}.bak"


def _snapshot_before_migrations(db_path, target_version):
    """Snapshot the database before pending migrations touch it.

    ``VACUUM INTO`` produces a consistent single-file copy even under WAL.
    A snapshot from an earlier (possibly failed) attempt at the same target
    version is kept as-is — it reflects an older, safer state than whatever
    partial progress the failed run left behind. Backup failure (disk full,
    read-only volume) is logged but never blocks startup: per-migration
    transactions still protect the live file.
    """
    backup = _backup_path(db_path, target_version)
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


_BACKUP_SUFFIX_RE = re.compile(r"\.pre-v(\d+)\.bak\Z")


def _prune_stale_backups(db_path, keep_version):
    # If the target-version snapshot never landed (VACUUM INTO failed, the
    # volume filled up, etc.), keep every older `.pre-v*.bak` — deleting them
    # would leave the upgraded database with no recovery snapshot at all.
    keep = _backup_path(db_path, keep_version)
    if not os.path.exists(keep):
        return
    prefix = db_path
    for path in glob.glob(glob.escape(prefix) + ".pre-v*.bak"):
        if path == keep:
            continue
        # Only prune snapshots for schema versions strictly older than the one
        # we're keeping. A `.pre-v{N}.bak` where N > keep_version was produced
        # by a newer build (e.g. after the user restored an older live catalog
        # while a later-version backup remained on disk) and may hold the
        # user's only copy of edits made under that later schema.
        suffix = path[len(prefix):]
        match = _BACKUP_SUFFIX_RE.match(suffix)
        if match is None:
            continue
        try:
            version = int(match.group(1))
        except ValueError:
            continue
        if version >= keep_version:
            continue
        with contextlib.suppress(OSError):
            os.remove(path)


def ensure_schema(db_path):
    """Initialize and migrate ``db_path`` once before request handling."""
    with _SCHEMA_LOCK:
        latest = _latest_version()
        current = _existing_user_version(db_path)
        # Refuse a database stamped by a newer Vireo before Database.__init__
        # runs its legacy ALTERs against it. IncompatibleDatabaseError (rather
        # than the bare RuntimeError _apply_pending raises) reaches main()'s
        # guided-exit handler, so the user gets "update Vireo" instead of a
        # raw traceback.
        if current is not None and current > latest:
            raise IncompatibleDatabaseError(
                db_path,
                cause=f"schema version {current} is newer than supported {latest}",
                newer=True,
            )
        if (
            current is not None
            and current < OLDEST_UPGRADABLE_VERSION
            and _has_tables(db_path)
        ):
            raise IncompatibleDatabaseError(
                db_path,
                cause=(
                    f"schema version {current} predates the oldest version "
                    f"this build can upgrade ({OLDEST_UPGRADABLE_VERSION})"
                ),
            )
        orphan_folder_grants = _unconverted_legacy_sync_only_folder_grants(db_path)
        if orphan_folder_grants:
            raise IncompatibleDatabaseError(
                db_path,
                cause=(
                    f"catalog has {orphan_folder_grants} unconverted row(s) "
                    "in the legacy workspace_sync_only_folders table; open "
                    "it with an earlier Vireo build first so those grants "
                    "can be rekeyed into workspace_sync_only_photos"
                ),
            )
        upgrading = current is not None and current < latest
        if upgrading:
            _snapshot_before_migrations(db_path, latest)
        with Database(db_path) as db:
            _apply_pending(db.conn)
        if upgrading:
            _prune_stale_backups(db_path, latest)
