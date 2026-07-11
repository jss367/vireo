import sqlite3
import threading

import pytest
import schema
from db import Database


def test_ensure_schema_applies_registry_and_validation(tmp_path):
    db_path = str(tmp_path / "vireo.db")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='schema_manager'"
        ).fetchone()[0] == "registry-v1"


def test_initialized_connection_does_not_run_schema_creation(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    def fail_if_called(_self):
        raise AssertionError("request connection attempted schema initialization")

    monkeypatch.setattr(Database, "_create_tables", fail_if_called)
    with Database(db_path, initialize_schema=False) as db:
        assert db._active_workspace_id is not None


def test_failed_registry_migration_rolls_back_version_and_data(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    def fail_after_write(conn):
        conn.execute(
            "INSERT INTO db_meta(key, value) VALUES ('partial_migration', 'bad')"
        )
        raise RuntimeError("simulated interruption")

    migration = schema.Migration(8, "interrupted", fail_after_write)
    monkeypatch.setattr(schema, "MIGRATIONS", (*schema.MIGRATIONS, migration))

    with pytest.raises(RuntimeError, match="simulated interruption"):
        schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7
        assert conn.execute(
            "SELECT 1 FROM db_meta WHERE key='partial_migration'"
        ).fetchone() is None


def test_concurrent_schema_startup_is_serialized(tmp_path):
    db_path = str(tmp_path / "vireo.db")
    errors = []

    def initialize():
        try:
            schema.ensure_schema(db_path)
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=initialize) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7


def test_navigation_restore_changes_only_consolidated_default(tmp_path):
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        workspace_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), workspace_id),
        )
        custom_id = db.create_workspace("Custom")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(["browse", "logs"]), custom_id),
        )
        db.conn.commit()

    # Reproduce a database that completed the now-reverted migration 6.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        conn.execute("PRAGMA user_version = 6")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = dict(conn.execute("SELECT name, tabs FROM workspaces").fetchall())
    assert json.loads(rows["Default"]) == schema._LEGACY_DEFAULT_TABS
    assert json.loads(rows["Custom"]) == ["browse", "logs"]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()[0] == "0"


def test_navigation_restore_only_touches_workspaces_v6_changed(tmp_path):
    """Preserve user-customized workspaces even when their tabs match the compact set.

    When migration 6 recorded which workspaces it rewrote, migration 7 must
    revert only those rows. A workspace the user manually customized to the
    same four-tab set (or that already matched it before v6 ran) was never
    touched by v6 and must not be clobbered by the restore.
    """
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        default_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), default_id),
        )
        # A second workspace whose tabs happen to match the compact set but
        # that v6 never modified (e.g. the user set it manually).
        untouched_id = db.create_workspace("UserCompact")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), untouched_id),
        )
        db.conn.commit()

    # Reproduce a database that completed v6 with per-row tracking of the
    # single workspace it actually changed.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated_ids", json.dumps([default_id])),
        )
        conn.execute("PRAGMA user_version = 6")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = dict(conn.execute("SELECT name, tabs FROM workspaces").fetchall())
    assert json.loads(rows["Default"]) == schema._LEGACY_DEFAULT_TABS
    assert json.loads(rows["UserCompact"]) == schema._PRIMARY_WORKFLOW_TABS
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()[0] == "0"
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
        ).fetchone() is None


def test_navigation_consolidation_records_changed_ids(tmp_path, monkeypatch):
    """Migration 6 stores the workspace IDs it rewrites so v7 can be precise."""
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        default_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._LEGACY_DEFAULT_TABS), default_id),
        )
        custom_id = db.create_workspace("Custom")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(["browse", "logs"]), custom_id),
        )
        db.conn.commit()

    # Run only migrations up through v6 so we can observe exactly what
    # v6 records before v7 would clear it.
    monkeypatch.setattr(
        schema,
        "MIGRATIONS",
        tuple(m for m in schema.MIGRATIONS if m.version <= 6),
    )
    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        recorded = conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
        ).fetchone()
        marker = conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()
    assert marker is not None and marker[0] == "1"
    assert recorded is not None
    assert json.loads(recorded[0]) == [default_id]
