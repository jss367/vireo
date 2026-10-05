import sqlite3
import threading

import pytest
import schema
from db import Database


def test_ensure_schema_stamps_baseline_on_fresh_db(tmp_path):
    db_path = str(tmp_path / "vireo.db")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == schema.BASELINE_VERSION
        assert [row[1] for row in conn.execute('PRAGMA table_info(location_gps_reviews)')] == [
            'photo_id', 'fingerprint', 'reviewed_at',
        ]


def test_database_stamps_baseline_on_a_database_it_creates(tmp_path):
    with Database(str(tmp_path / "vireo.db")) as db:
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == schema.BASELINE_VERSION


def test_ensure_schema_refuses_catalog_older_than_retired_migrations(tmp_path):
    """A populated catalog below the oldest upgradable version is refused, not
    stamped as upgraded without the retired migrations it still needs."""
    from db import IncompatibleDatabaseError

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    old_version = schema.OLDEST_UPGRADABLE_VERSION - 1
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {old_version}")

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        schema.ensure_schema(db_path)

    assert not excinfo.value.newer
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == old_version


def test_ensure_schema_refuses_catalog_with_unconverted_legacy_folder_grants(tmp_path):
    """A populated catalog carrying rows in the retired
    ``workspace_sync_only_folders`` table is refused: those grants were only
    convertible by the removed migration, so opening it would silently
    discard them. Catalogs without the legacy table (or with it empty) stay
    unaffected."""
    from db import IncompatibleDatabaseError

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE workspace_sync_only_folders ("
            "workspace_id INTEGER NOT NULL, folder_id INTEGER NOT NULL)"
        )
        conn.execute(
            "INSERT INTO workspace_sync_only_folders(workspace_id, folder_id)"
            " VALUES (1, 2)"
        )
        conn.commit()

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        schema.ensure_schema(db_path)

    assert "workspace_sync_only_folders" in str(excinfo.value)
    assert not excinfo.value.newer

    # An empty legacy table does not trip the guard.
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM workspace_sync_only_folders")
        conn.commit()
    schema.ensure_schema(db_path)


def test_ensure_schema_initializes_an_empty_existing_file(tmp_path):
    db_path = tmp_path / "vireo.db"
    sqlite3.connect(db_path).close()

    schema.ensure_schema(str(db_path))

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == schema.BASELINE_VERSION


def test_registry_migration_above_baseline_applies_once(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    calls = []

    def add_marker(conn):
        calls.append(1)
        conn.execute("INSERT INTO db_meta(key, value) VALUES ('next_migration', 'ok')")

    migration = schema.Migration(schema.BASELINE_VERSION + 1, "next", add_marker)
    monkeypatch.setattr(schema, "MIGRATIONS", (migration,))

    schema.ensure_schema(db_path)
    schema.ensure_schema(db_path)

    assert calls == [1]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == schema.BASELINE_VERSION + 1
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='next_migration'"
        ).fetchone()[0] == "ok"


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

    migration = schema.Migration(13, "interrupted", fail_after_write)
    monkeypatch.setattr(schema, "MIGRATIONS", (*schema.MIGRATIONS, migration))

    with pytest.raises(RuntimeError, match="simulated interruption"):
        schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 12
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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 12


def test_ensure_schema_backs_up_before_pending_migrations(tmp_path):
    """An existing DB with pending registry migrations is snapshotted before
    any migration runs, so a bad migration can't destroy the only copy."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {schema.OLDEST_UPGRADABLE_VERSION}")

    schema.ensure_schema(db_path)

    latest = schema._latest_version()
    backup_path = f"{db_path}.pre-v{latest}.bak"
    assert os.path.exists(backup_path)
    # The snapshot reflects the pre-migration state.
    with sqlite3.connect(backup_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == schema.OLDEST_UPGRADABLE_VERSION
        assert conn.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0] >= 1


def test_ensure_schema_no_backup_for_fresh_or_current_db(tmp_path):
    """No snapshot for a brand-new DB, and none when the schema is already
    at the latest version (normal startup must not accrete backups)."""
    import glob

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    assert glob.glob(f"{db_path}*.bak") == []

    schema.ensure_schema(db_path)
    assert glob.glob(f"{db_path}*.bak") == []


def test_ensure_schema_prunes_older_backups(tmp_path):
    """Only the most recent pre-migration snapshot is kept."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema._latest_version()
    stale_backup = f"{db_path}.pre-v{latest - 1}.bak"
    with open(stale_backup, "w") as f:
        f.write("old snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {schema.OLDEST_UPGRADABLE_VERSION}")

    schema.ensure_schema(db_path)

    assert os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert not os.path.exists(stale_backup)


def test_ensure_schema_keeps_older_backups_when_snapshot_fails(tmp_path, monkeypatch):
    """If the pre-migration snapshot can't be written (VACUUM INTO fails,
    volume full, etc.), an older `.pre-v*.bak` from a previous successful
    run must survive — otherwise the upgrade completes with no recovery
    snapshot at all."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema._latest_version()
    stale_backup = f"{db_path}.pre-v{latest - 1}.bak"
    with open(stale_backup, "w") as f:
        f.write("older snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {schema.OLDEST_UPGRADABLE_VERSION}")

    # Simulate _snapshot_before_migrations failing silently (its except
    # branch already swallows sqlite3.Error / OSError and logs a warning).
    monkeypatch.setattr(schema, "_snapshot_before_migrations", lambda *a, **k: None)

    schema.ensure_schema(db_path)

    assert not os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert os.path.exists(stale_backup)


def test_ensure_schema_newer_db_raises_incompatible_database_error(tmp_path):
    """Opening a DB stamped by a newer Vireo raises the friendly
    IncompatibleDatabaseError (caught by main's guided-exit handler) instead
    of an anonymous RuntimeError crash — and does not mutate the file."""
    from db import IncompatibleDatabaseError

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 99")

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        schema.ensure_schema(db_path)

    assert "newer" in str(excinfo.value)
    assert excinfo.value.db_path == db_path
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 99


def test_load_taxonomy_cli_refuses_newer_db(tmp_path, monkeypatch, capsys):
    """``--load-taxonomy`` must not run legacy DDL against a catalog stamped
    by a newer Vireo. Previously it bypassed ensure_schema and constructed
    Database(args.db) directly, mutating the file and burying the version
    mismatch in an OperationalError; the guided-exit handler should now fire
    the same structured signal here as it does for the normal startup path."""
    import json as _json

    import app as vireo_app

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 99")

    monkeypatch.setattr(
        "sys.argv",
        ["vireo", "--db", db_path, "--thumb-dir", str(tmp_path / "thumbs"),
         "--load-taxonomy"],
    )
    # Guard against actually touching the DB or the network if the fix
    # regresses and control ever reaches Database(args.db)/load_taxonomy.
    def _boom(*_a, **_kw):  # pragma: no cover - regression guard
        raise AssertionError("--load-taxonomy touched the DB before ensure_schema")

    monkeypatch.setattr("db.Database", _boom)
    # main() would otherwise attach a handler for the real ~/.vireo/vireo.log.
    monkeypatch.setattr(vireo_app, "_setup_file_logging", lambda: None)

    with pytest.raises(SystemExit) as excinfo:
        vireo_app.main()
    assert excinfo.value.code == 3

    captured = capsys.readouterr()
    payload = _json.loads(captured.err.strip().splitlines()[-1])
    assert payload["error"] == "incompatible_database"
    assert payload["newer"] is True
    assert payload["db_path"] == db_path

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 99


def test_ensure_schema_keeps_later_version_snapshots(tmp_path):
    """Restoring an older live database while a later-version backup remains
    (e.g. `.pre-v9.bak` beside a v7 file, then launching this v8 build) must
    keep the newer snapshot — it may hold the user's only copy of edits made
    under that later schema. Only strictly older snapshots may be pruned."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema._latest_version()
    older_backup = f"{db_path}.pre-v{latest - 1}.bak"
    newer_backup = f"{db_path}.pre-v{latest + 1}.bak"
    with open(older_backup, "w") as f:
        f.write("older snapshot")
    with open(newer_backup, "w") as f:
        f.write("later-version snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {schema.OLDEST_UPGRADABLE_VERSION}")

    schema.ensure_schema(db_path)

    assert os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert not os.path.exists(older_backup)
    # The later-version backup must survive — it may be irreplaceable.
    assert os.path.exists(newer_backup)
