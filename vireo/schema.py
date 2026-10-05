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


# The schema ``create_tables`` builds. Migrations 5-12 were retired once every
# catalog had applied them, and ``create_tables`` now creates their end state
# directly, so ``_apply_pending`` stamps this version on any catalog below it.
# The next schema change is ``Migration(BASELINE_VERSION + 1, ...)``.
BASELINE_VERSION = 12

MIGRATIONS = ()


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
        upgrading = current is not None and current < latest
        if upgrading:
            _snapshot_before_migrations(db_path, latest)
        with Database(db_path) as db:
            _apply_pending(db.conn)
        if upgrading:
            _prune_stale_backups(db_path, latest)
