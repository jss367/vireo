"""One-time database schema initialization and ordered migrations.

The legacy canonical-schema code remains in ``Database`` while it is split
into discrete historical migrations.  This module is the startup boundary:
web requests open an initialized database and never perform schema work.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass

from db import Database

_SCHEMA_LOCK = threading.Lock()


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[sqlite3.Connection], None]
    validate: Callable[[sqlite3.Connection], None] | None = None


def _establish_startup_boundary(conn):
    """Version marker for the first migration managed by this registry."""
    conn.execute(
        "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
        ("schema_manager", "registry-v1"),
    )


def _validate_startup_boundary(conn):
    row = conn.execute(
        "SELECT value FROM db_meta WHERE key='schema_manager'"
    ).fetchone()
    if row is None or row[0] != "registry-v1":
        raise RuntimeError("schema migration validation failed: registry marker missing")


_LEGACY_DEFAULT_TABS = [
    "import", "browse", "pipeline", "pipeline_review",
    "review", "cull", "jobs", "highlights", "misses", "storage", "settings",
]
_PRIMARY_WORKFLOW_TABS = ["import", "pipeline", "review", "browse"]


def _consolidate_default_navigation(conn):
    """Simplify only the untouched legacy default; preserve custom tab sets."""
    import json

    rows = conn.execute("SELECT id, tabs FROM workspaces").fetchall()
    changed_ids = []
    for workspace_id, raw_tabs in rows:
        try:
            tabs = json.loads(raw_tabs) if raw_tabs else None
        except (TypeError, ValueError):
            continue
        if tabs == _LEGACY_DEFAULT_TABS:
            conn.execute(
                "UPDATE workspaces SET tabs=? WHERE id=?",
                (json.dumps(_PRIMARY_WORKFLOW_TABS), workspace_id),
            )
            changed_ids.append(workspace_id)
    if changed_ids:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        # Record exactly which workspaces this migration rewrote so the
        # reversal in migration 7 can restore only those rows and leave
        # any workspace the user later customized to the same tab set alone.
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated_ids", json.dumps(changed_ids)),
        )


def _restore_direct_default_navigation(conn):
    """Restore the direct tabs changed by migration 6; preserve custom sets."""
    import json

    marker = conn.execute(
        "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
    ).fetchone()
    if marker is None or marker[0] != "1":
        return

    tracked_row = conn.execute(
        "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
    ).fetchone()
    tracked_ids = None
    if tracked_row is not None:
        try:
            tracked_ids = {int(x) for x in json.loads(tracked_row[0])}
        except (TypeError, ValueError):
            tracked_ids = None

    rows = conn.execute("SELECT id, tabs FROM workspaces").fetchall()
    for workspace_id, raw_tabs in rows:
        # When the consolidation migration recorded which rows it changed,
        # only revert those specific workspaces. Workspaces the user later
        # customized to the compact tab set (or that already matched it
        # before v6 ran) were never touched by v6 and must be preserved.
        if tracked_ids is not None and workspace_id not in tracked_ids:
            continue
        try:
            tabs = json.loads(raw_tabs) if raw_tabs else None
        except (TypeError, ValueError):
            continue
        if tabs == _PRIMARY_WORKFLOW_TABS:
            conn.execute(
                "UPDATE workspaces SET tabs=? WHERE id=?",
                (json.dumps(_LEGACY_DEFAULT_TABS), workspace_id),
            )
    conn.execute(
        "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
        ("navigation_consolidated", "0"),
    )
    conn.execute(
        "DELETE FROM db_meta WHERE key='navigation_consolidated_ids'"
    )


MIGRATIONS = (
    Migration(
        version=5,
        name="establish-versioned-schema-manager",
        apply=_establish_startup_boundary,
        validate=_validate_startup_boundary,
    ),
    Migration(
        version=6,
        name="consolidate-untouched-default-navigation",
        apply=_consolidate_default_navigation,
    ),
    Migration(
        version=7,
        name="restore-direct-default-navigation",
        apply=_restore_direct_default_navigation,
    ),
)


def _apply_pending(conn):
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    latest = MIGRATIONS[-1].version if MIGRATIONS else current
    if current > latest:
        raise RuntimeError(
            f"database schema version {current} is newer than supported {latest}"
        )

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


def ensure_schema(db_path):
    """Initialize and migrate ``db_path`` once before request handling."""
    with _SCHEMA_LOCK, Database(db_path) as db:
        _apply_pending(db.conn)
