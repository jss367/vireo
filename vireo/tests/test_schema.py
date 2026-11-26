import sqlite3
import threading

import pytest
import schema
from canonical_schema import SCHEMA_VERSION
from db import Database
from services import photo_ingest, workspace_folders


def test_ensure_schema_stamps_newest_version_on_fresh_db(tmp_path):
    db_path = str(tmp_path / "vireo.db")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert [row[1] for row in conn.execute('PRAGMA table_info(location_gps_reviews)')] == [
            'photo_id', 'fingerprint', 'reviewed_at',
        ]


def test_database_stamps_newest_version_on_a_database_it_creates(tmp_path):
    with Database(str(tmp_path / "vireo.db")) as db:
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_registry_migration_above_baseline_applies_once(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    calls = []

    def add_marker(conn):
        calls.append(1)
        conn.execute("INSERT INTO db_meta(key, value) VALUES ('next_migration', 'ok')")

    migration = schema.Migration(SCHEMA_VERSION + 1, "next", add_marker)
    monkeypatch.setattr(schema, "MIGRATIONS", (migration,))

    schema.ensure_schema(db_path)
    schema.ensure_schema(db_path)

    assert calls == [1]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION + 1
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

    migration = schema.Migration(SCHEMA_VERSION + 1, "interrupted", fail_after_write)
    monkeypatch.setattr(schema, "MIGRATIONS", (*schema.MIGRATIONS, migration))

    with pytest.raises(RuntimeError, match="simulated interruption"):
        schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


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

    # Guard the prune on its own: a snapshot step that returns without
    # writing the target-version backup must not cost the older ones.
    monkeypatch.setattr(schema, "_snapshot_before_migrations", lambda *a, **k: None)

    schema.ensure_schema(db_path)

    assert not os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert os.path.exists(stale_backup)


def _db_one_upgrade_behind(tmp_path):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {schema.OLDEST_UPGRADABLE_VERSION}")
    return db_path


def _user_version(db_path):
    with sqlite3.connect(db_path) as conn:
        return conn.execute("PRAGMA user_version").fetchone()[0]


def test_ensure_schema_refuses_to_migrate_without_room_for_backup(tmp_path, monkeypatch):
    """Too little free space for the backup stops the upgrade before any
    migration runs, and the error tells the user how much to free."""
    import collections
    import glob

    db_path = _db_one_upgrade_behind(tmp_path)
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(schema.shutil, "disk_usage", lambda _path: usage(0, 0, 1024))

    with pytest.raises(schema.MigrationBackupError) as excinfo:
        schema.ensure_schema(db_path)

    message = str(excinfo.value)
    assert db_path in message
    assert "has 0.0 GB free" in message
    assert "Free at least" in message
    assert "The catalog was not changed" in message
    assert _user_version(db_path) == schema.OLDEST_UPGRADABLE_VERSION
    assert glob.glob(f"{db_path}.pre-v*") == []


def test_ensure_schema_near_threshold_shortfall_reports_nonzero_amount(tmp_path, monkeypatch):
    """A deficit smaller than 0.05 GB must not round to "Free at least 0.0 GB":
    the instruction stays actionable by falling back to MB."""
    import collections

    db_path = _db_one_upgrade_behind(tmp_path)
    needed = schema._backup_space_needed(db_path)
    # Leave the user 5 MB short of what the backup needs.
    free = max(0, needed - 5 * 1024 * 1024)
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(schema.shutil, "disk_usage", lambda _path: usage(0, 0, free))

    with pytest.raises(schema.MigrationBackupError) as excinfo:
        schema.ensure_schema(db_path)

    message = str(excinfo.value)
    assert "Free at least" in message
    assert "0.0 GB on that drive" not in message
    assert " MB on that drive" in message


def test_backup_space_needed_excludes_freelist_pages(tmp_path):
    """A catalog whose deleted rows left substantial freelist space produces
    a smaller backup than its raw file length; the preflight must size the
    estimate against the compacted pages, not ``os.path.getsize``."""
    import os as _os

    db_path = str(tmp_path / "vireo.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("CREATE TABLE big (data BLOB)")
        conn.executemany(
            "INSERT INTO big VALUES (?)", [(b"x" * 1024,) for _ in range(5000)]
        )
        conn.commit()
        conn.execute("DELETE FROM big")
        conn.commit()
    raw_size = _os.path.getsize(db_path)

    needed = schema._backup_space_needed(db_path)

    # The old raw-size estimate stayed at the un-compacted file length plus
    # headroom; the new one must come in below that because freelist pages
    # are dropped by VACUUM INTO.
    old_estimate = raw_size + max(
        int(raw_size * schema._BACKUP_HEADROOM_FRACTION),
        schema._BACKUP_HEADROOM_MIN_BYTES,
    )
    assert needed < old_estimate, (
        f"needed={needed}, old_estimate={old_estimate}, raw_size={raw_size}"
    )


def test_backup_space_needed_ignores_pinned_wal(tmp_path):
    """Another connection can pin a WAL much larger than the live catalog.
    Those bytes already sit on the volume, so the preflight must not add
    them again — otherwise a tiny catalog with a big pinned WAL is refused
    indefinitely even when the backup would fit."""
    db_path = str(tmp_path / "vireo.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("CREATE TABLE t (data BLOB)")
        conn.commit()

    baseline = schema._backup_space_needed(db_path)

    # A WAL left behind by another connection — often many times the live
    # catalog when it has been pinned through repeated updates.
    with open(db_path + "-wal", "wb") as wal:
        wal.write(b"\0" * (128 * 1024 * 1024))

    assert schema._backup_space_needed(db_path) == baseline


def test_format_minimum_to_free_rounds_up_small_deficits():
    """A 1-byte shortfall still asks the user to free at least 1 MB, and
    sub-tenth-of-GB values are rendered in MB instead of rounding to 0.0 GB."""
    assert schema._format_minimum_to_free(1) == "1 MB"
    assert schema._format_minimum_to_free(5 * 1024 * 1024) == "5 MB"
    # Exactly 1 GB tips into GB formatting.
    assert schema._format_minimum_to_free(1024 ** 3) == "1.0 GB"
    # 1 GB + 1 byte rounds up to the next tenth.
    assert schema._format_minimum_to_free(1024 ** 3 + 1) == "1.1 GB"


def test_ensure_schema_refuses_to_migrate_when_backup_write_fails(tmp_path, monkeypatch):
    """A backup that fails after the space check (read-only volume, the
    disk filling during the copy) stops the upgrade and leaves no partial
    backup behind."""
    import errno
    import glob

    db_path = _db_one_upgrade_behind(tmp_path)

    def fail_replace(_src, _dst):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(schema, "replace_file", fail_replace)

    with pytest.raises(schema.MigrationBackupError) as excinfo:
        schema.ensure_schema(db_path)

    assert "No space left on device" in str(excinfo.value)
    assert _user_version(db_path) == schema.OLDEST_UPGRADABLE_VERSION
    assert glob.glob(f"{db_path}.pre-v*") == []


def test_ensure_schema_existing_backup_skips_space_check(tmp_path, monkeypatch):
    """A backup from an earlier attempt at the same target already protects
    the catalog, so a full disk does not block finishing the upgrade."""
    db_path = _db_one_upgrade_behind(tmp_path)
    latest = schema._latest_version()
    with open(f"{db_path}.pre-v{latest}.bak", "w") as f:
        f.write("earlier snapshot")

    def no_disk_usage(_path):
        raise AssertionError("free space checked although the backup exists")

    monkeypatch.setattr(schema.shutil, "disk_usage", no_disk_usage)

    schema.ensure_schema(db_path)

    assert _user_version(db_path) == latest


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


def test_load_taxonomy_cli_structured_exit_on_backup_failure(tmp_path, monkeypatch, capsys):
    """``--load-taxonomy`` also opens the catalog through ensure_schema, so
    a pre-migration backup failure (full disk, read-only volume) must emit
    the same structured startup_failed signal create_app does instead of
    letting a raw MigrationBackupError traceback escape."""
    import collections
    import json as _json

    import app as vireo_app

    db_path = _db_one_upgrade_behind(tmp_path)
    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(schema.shutil, "disk_usage", lambda _path: usage(0, 0, 1024))

    monkeypatch.setattr(
        "sys.argv",
        ["vireo", "--db", db_path, "--thumb-dir", str(tmp_path / "thumbs"),
         "--load-taxonomy"],
    )
    # Guard against actually touching the DB or the network if the fix
    # regresses and control ever reaches Database(args.db)/load_taxonomy.
    def _boom(*_a, **_kw):  # pragma: no cover - regression guard
        raise AssertionError("--load-taxonomy touched the DB after backup failure")

    monkeypatch.setattr("db.Database", _boom)
    monkeypatch.setattr(vireo_app, "_setup_file_logging", lambda: None)

    with pytest.raises(SystemExit) as excinfo:
        vireo_app.main()
    assert excinfo.value.code == 2

    captured = capsys.readouterr()
    payload = _json.loads(captured.err.strip().splitlines()[-1])
    assert payload["error"] == "startup_failed"
    assert db_path in payload["reason"]
    assert "backup" in payload["reason"].lower()

    assert _user_version(db_path) == schema.OLDEST_UPGRADABLE_VERSION


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


def test_newest_registry_migration_is_the_version_create_tables_stamps():
    """``create_tables`` builds every migration's end state and stamps a fresh
    database with the newest one, so the two numbers must move together."""
    assert schema.MIGRATIONS[-1].version == SCHEMA_VERSION
    assert schema._latest_version() == SCHEMA_VERSION


def test_migration_versions_are_unique_and_strictly_increasing():
    versions = [migration.version for migration in schema.MIGRATIONS]
    assert versions == sorted(set(versions))


def test_version_23_import_catalog_gains_identity_state_without_changing_import_history(tmp_path):
    path = str(tmp_path / 'import-v23.db')
    with Database(path) as db:
        fid = workspace_folders.add_folder(db, '/photos', name='Photos')
        pid = photo_ingest.add_photo(db, fid, 'bird.jpg', '.jpg', 100, 1.0)
        cid = db.collections.add_import('Original imported photos', [pid, pid + 10])
        db.commit()
        original = dict(db.collections.import_provenance(cid))
    with sqlite3.connect(path) as conn:
        conn.execute('DROP TABLE photo_identity_alternatives')
        conn.execute('PRAGMA user_version = 23')
        # Exercise the registry directly before Database's idempotent schema
        # setup can create the missing table on its own.
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert conn.execute('SELECT COUNT(*) FROM photo_identity_alternatives').fetchone()[0] == 0
    schema.ensure_schema(path)
    with Database(path, initialize_schema=False) as db:
        assert dict(db.collections.import_provenance(cid)) == original
        assert original['original_import_count'] == 2
        assert original['import_recorded_at']
        assert db.identity_alternatives.photo_ids() == set()


def test_same_shot_review_migration_is_idempotent_and_preserves_notes(tmp_path):
    path = str(tmp_path / 'pre-same-shot-review.db')
    Database(path).close()
    with sqlite3.connect(path) as conn:
        conn.execute('DROP TABLE same_shot_reviews')
        conn.execute('PRAGMA user_version = 26')
        schema._apply_pending(conn)
        conn.execute("INSERT INTO same_shot_reviews (group_key,review_key,decision) VALUES ('g','r','keep_all')")
        conn.commit()
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert conn.execute('SELECT group_key,review_key,decision FROM same_shot_reviews').fetchone() == ('g', 'r', 'keep_all')


def test_version_22_catalog_runs_import_and_identity_registry_migrations(tmp_path):
    path = str(tmp_path / 'pre-import-v22.db')
    with Database(path) as db:
        collection = db.collections.add('Historical import', '[{"field":"photo_ids","value":[1,2]}]')
        db.commit()
    with sqlite3.connect(path) as conn:
        conn.execute('DROP TABLE photo_identity_alternatives')
        conn.execute('ALTER TABLE collections DROP COLUMN original_import_count')
        conn.execute('ALTER TABLE collections DROP COLUMN import_recorded_at')
        conn.execute('PRAGMA user_version = 22')
        # Run the registry before normal startup can create missing tables.
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert conn.execute('SELECT COUNT(*) FROM photo_identity_alternatives').fetchone()[0] == 0
        row = conn.execute(
            'SELECT name, rules, original_import_count, import_recorded_at FROM collections WHERE id=?',
            (collection,),
        ).fetchone()
        assert row == ('Historical import', '[{"field":"photo_ids","value":[1,2]}]', None, None)
        assert conn.execute('PRAGMA foreign_key_check').fetchall() == []


@pytest.mark.parametrize("stamped", [11, 12])
def test_catalog_before_companion_mtime_gains_the_column(tmp_path, stamped):
    """A v11/v12 catalog's companion_identities has no file_mtime; the
    migration adds it and keeps every stored identity."""
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE companion_identities")
        conn.execute(
            "CREATE TABLE companion_identities ("
            "photo_id INTEGER PRIMARY KEY REFERENCES photos(id) ON DELETE CASCADE,"
            " filename TEXT NOT NULL, file_size INTEGER, timestamp TEXT,"
            " file_hash TEXT)"
        )
        conn.execute(
            "INSERT INTO companion_identities VALUES (7, 'IMG.jpg', 10, NULL, 'h')"
        )
        conn.execute(f"PRAGMA user_version = {stamped}")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute(
            "SELECT photo_id, filename, file_size, file_hash, file_mtime"
            " FROM companion_identities"
        ).fetchall() == [(7, "IMG.jpg", 10, "h", None)]


def test_v13_companion_identities_remain_trusted_after_sync_state_migration(tmp_path):
    """Existing NULL mtimes are metadata retries, not recovered byte identities."""
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE companion_identities")
        conn.execute(
            "CREATE TABLE companion_identities (photo_id INTEGER PRIMARY KEY,"
            " filename TEXT NOT NULL, file_size INTEGER, timestamp TEXT,"
            " file_hash TEXT, file_mtime REAL)"
        )
        conn.executemany(
            "INSERT INTO companion_identities VALUES (?, ?, ?, ?, ?, ?)",
            [(7, "IMG.jpg", 10, None, "h", None),
             (8, "OTHER.jpg", 20, "2024-01-15", "other", 123.0)],
        )
        conn.execute("PRAGMA user_version = 13")

    schema.ensure_schema(db_path)
    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute(
            "SELECT photo_id, filename, file_size, timestamp, file_hash,"
            " file_mtime, needs_sync FROM companion_identities ORDER BY photo_id"
        ).fetchall() == [
            (7, "IMG.jpg", 10, None, "h", None, 0),
            (8, "OTHER.jpg", 20, "2024-01-15", "other", 123.0, 0),
        ]


def test_version_14_catalog_gains_paired_preview_cache(tmp_path):
    path = str(tmp_path / "pre-paired.db")
    with Database(path) as db:
        db.conn.execute("DROP TABLE paired_preview_cache")
        db.conn.execute("PRAGMA user_version=14")
        db.conn.commit()
        schema._apply_pending(db.conn)
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert [r[1] for r in db.conn.execute("PRAGMA table_info(paired_preview_cache)")] == [
            "filename", "photo_id", "bytes", "last_access_at",
        ]
        assert db.conn.execute("PRAGMA foreign_key_list(paired_preview_cache)").fetchone()[2] == "photos"


def test_version_15_catalog_gains_prediction_history_instances_without_changing_predictions(tmp_path):
    db_path = str(tmp_path / 'vireo.db')
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        fid = workspace_folders.add_folder(db, '/photos', name='Photos')
        pid = photo_ingest.add_photo(db, fid, 'bird.jpg', '.jpg', 100, 1.0)
        did = db.detections.save(pid, [
            {'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'},
        ], detector_model='test')[0]
        db.predictions.add(did, 'Bald Eagle', .9, 'test')
        db.commit()
        original = [tuple(row) for row in db.conn.execute('SELECT * FROM predictions')]
        db.conn.execute('DROP TABLE prediction_history_instances')
        db.conn.execute('PRAGMA user_version = 15')
        db.commit()
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        assert db.conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert [tuple(row) for row in db.conn.execute('SELECT * FROM predictions')] == original
        db.edit_history.ensure_prediction_instances([original[0][0]])
        db.commit()
        assert len(db.conn.execute('SELECT instance_id FROM prediction_history_instances').fetchone()[0]) == 32
        assert db.conn.execute('PRAGMA foreign_key_check').fetchall() == []


def test_version_16_catalog_backfills_grouping_snapshot_byte_counts(tmp_path):
    db_path = str(tmp_path / 'vireo.db')
    schema.ensure_schema(db_path)
    payload = '{"before": "caf\u00e9 \U0001f985", "after": []}'
    with Database(db_path, initialize_schema=False) as db:
        edit = db.record_edit('pipeline_grouping', 'Legacy grouping', '{}', [])
        db.commit()
        db.conn.execute('ALTER TABLE edit_history_payloads DROP COLUMN bytes')
        db.conn.execute('INSERT INTO edit_history_payloads(edit_id, payload) VALUES (?, ?)', (edit, payload))
        db.conn.execute('PRAGMA user_version = 16')
        db.commit()
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        row = db.conn.execute('SELECT payload, bytes FROM edit_history_payloads WHERE edit_id=?', (edit,)).fetchone()
        assert row['payload'] == payload
        assert row['bytes'] == len(payload.encode('utf-8'))
        assert db.storage.grouping_history()['bytes'] == row['bytes']
        assert db.conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        assert db.conn.execute('PRAGMA foreign_key_check').fetchall() == []


def test_version_17_catalog_gains_the_formerly_lazy_tables_and_keeps_job_history(tmp_path):
    """A catalog whose job runner created ``job_history`` before
    ``workspace_id`` (and every later column) existed, and that never wrote
    a preview-cache marker or an identity repair, opens without error (the
    index and trigger on ``workspace_id`` wait for the column) and comes out
    with every table, column and row."""
    db_path = str(tmp_path / 'vireo.db')
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        for name in ('job_history', 'preview_cache_invalidations', 'species_identity_repairs'):
            db.conn.execute(f'DROP TABLE {name}')
        db.conn.execute('DROP INDEX idx_taxa_common_names_name_nocase')
        db.conn.execute("""CREATE TABLE job_history (
            id TEXT PRIMARY KEY, type TEXT, status TEXT, started_at TEXT,
            finished_at TEXT, duration REAL, result TEXT,
            error_count INTEGER DEFAULT 0, config TEXT
        )""")
        db.conn.execute(
            "INSERT INTO job_history (id, type, status, started_at) "
            "VALUES ('old', 'scan', 'completed', '2026-01-01T00:00:00')",
        )
        db.conn.execute('PRAGMA user_version = 17')
        db.commit()
    # Plain opens (create_tables) must not trip over the missing column.
    Database(db_path).close()
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        assert db.conn.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION
        columns = [r[1] for r in db.conn.execute('PRAGMA table_info(job_history)')]
        assert columns[:10] == [
            'id', 'type', 'status', 'started_at', 'finished_at', 'duration',
            'result', 'error_count', 'config', 'workspace_id',
        ]
        assert set(columns) >= {'tree', 'summary', 'resource_wait_seconds',
                                'resource_wait_count', 'progress'}
        assert db.job_history.get('old')['summary'] == ''
        assert db.job_history.get('old')['workspace_id'] is None
        objects = {
            row[0] for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'index', 'trigger')"
            )
        }
        assert objects >= {
            'preview_cache_invalidations', 'species_identity_repairs',
            'idx_job_history_workspace', 'job_history_require_workspace',
            'idx_taxa_common_names_name_nocase',
        }
        # The ownership trigger came along: a row for a deleted workspace is dropped.
        db.conn.execute(
            "INSERT INTO job_history (id, type, status, workspace_id) VALUES ('ghost', 'scan', 'completed', 999999)"
        )
        assert db.job_history.get('ghost') is None
        assert db.conn.execute('PRAGMA foreign_key_check').fetchall() == []


def test_fresh_catalog_needs_no_formerly_lazy_table_migration(tmp_path):
    """``create_tables`` builds migration 18's end state, so the registry
    entry is a no-op on a fresh database: running it changes nothing."""
    db_path = str(tmp_path / 'fresh.db')
    schema.ensure_schema(db_path)
    with Database(db_path, initialize_schema=False) as db:
        before = db.conn.execute('SELECT type, name, sql FROM sqlite_master ORDER BY name').fetchall()
        schema._add_formerly_lazy_tables(db.conn)
        schema._validate_formerly_lazy_tables(db.conn)
        assert db.conn.execute('SELECT type, name, sql FROM sqlite_master ORDER BY name').fetchall() == before


def test_subject_comparison_cache_migrates_without_changing_photographer_choices(tmp_path):
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE subject_ranking_analysis")
        conn.execute("PRAGMA user_version=21")
        conn.execute("INSERT INTO db_meta(key,value) VALUES ('photographer_choice_test','preserved')")
    schema.ensure_schema(path)
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute("SELECT value FROM db_meta WHERE key='photographer_choice_test'").fetchone()[0] == 'preserved'
        assert [r[1] for r in conn.execute("PRAGMA table_info(subject_ranking_analysis)")] == [
            "detection_id", "source_key", "features", "mask_png"]


@pytest.mark.parametrize("version", [38, 39, 40, 41, 42, 43, 44, 45])
def test_catalog_unmeasures_the_detail_read_another_way(tmp_path, version):
    """Historical nonempty measurements lack rendering provenance at every
    subject size; masks, empty-mask zeros and pending values are kept."""
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    # photo id: (detail, subject px, frame width, frame height)
    rows = {
        1: (19413.16, 259.2, 5408, 3608),    # under the scale
        2: (708.37, 517.9, 5408, 3608),      # under a full-size preview's coverage
        3: (9026.48, 518.0, 5408, 3608),     # middle band can still have used a reduced JPEG
        4: (24410.87, 1015.7, 5408, 3608),
        5: (0.0, None, 5408, 3608),          # empty mask, nothing was read: kept
        6: (5462.9, 2137.0, 5408, 3608),     # fit the default 1536 px proxy
        7: (4000.0, 1351.0, 5408, 3608),     # just under a quarter, still lacks preview provenance
        8: (4000.0, 1400.0, 3608, 5408),     # portrait frame, the long edge decides
        10: (4000.0, 1352.0, 5408, 3608),    # a quarter of the frame: fit a 2048 px proxy
        9: (4000.0, 900.0, None, None),      # frame size unrecorded
        11: (4000.0, None, 5408, 3608),     # nonempty score without a recorded subject size
        12: (None, None, 5408, 3608),       # already pending
    }
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO folders (id, path) VALUES (1, '/photos')")
        for photo_id, (detail, px, width, height) in rows.items():
            conn.execute(
                "INSERT INTO photos (id, folder_id, filename, width, height, subject_detail, subject_detail_px) "
                "VALUES (?, 1, ?, ?, ?, ?, ?)",
                (photo_id, f"{photo_id}.nef", width, height, detail, px),
            )
            for variant in ("sam2-large", "sam2-small"):
                conn.execute(
                    "INSERT INTO photo_masks (photo_id, variant, path, created_at, detector_model, prompt_x, prompt_y, "
                    "prompt_w, prompt_h, subject_detail, subject_detail_px) VALUES (?, ?, ?, 0, 'md', 0, 0, 1, 1, ?, ?)",
                    (photo_id, variant, f"/masks/{photo_id}.{variant}.png", detail, px),
                )
        conn.execute(f"PRAGMA user_version={version}")
    schema.ensure_schema(path)
    schema.ensure_schema(path)
    kept = {photo_id: rows[photo_id][:2] for photo_id in (5, 12)}
    cleared = dict.fromkeys(set(rows) - kept.keys(), (None, None))
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        got = {row[0]: (row[1], row[2]) for row in conn.execute("SELECT id, subject_detail, subject_detail_px FROM photos")}
        assert got == {**cleared, **kept}
        for variant in ("sam2-large", "sam2-small"):
            got = {row[0]: (row[1], row[2]) for row in conn.execute(
                "SELECT photo_id, subject_detail, subject_detail_px FROM photo_masks WHERE variant=?", (variant,))}
            assert got == {**cleared, **kept}, variant
        # The masks themselves stay: only the measurement is pending again.
        assert conn.execute("SELECT COUNT(*) FROM photo_masks WHERE path IS NOT NULL").fetchone()[0] == 24


@pytest.mark.parametrize("version", [38, 39, 40, 41, 42, 43, 44, 45])
def test_middle_band_detail_is_invalidated_without_reading_the_raw(tmp_path, version):
    """A 1000 px subject could have used a 6000x3376 cropped JPEG on a
    6000x4000 sensor. Preview dimensions were not stored with the score."""
    path = str(tmp_path / "synthetic.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO folders (id, path) VALUES (1, '/synthetic')")
        conn.execute(
            "INSERT INTO photos (id, folder_id, filename, width, height, subject_detail, subject_detail_px) "
            "VALUES (1, 1, 'fake.nef', 6000, 4000, 1234, 1000)"
        )
        for variant in ("sam2-large", "sam2-small"):
            conn.execute(
                "INSERT INTO photo_masks (photo_id, variant, path, created_at, detector_model, "
                "prompt_x, prompt_y, prompt_w, prompt_h, subject_detail, subject_detail_px) "
                "VALUES (1, ?, ?, 0, 'md', 0, 0, 1, 1, 1234, 1000)",
                (variant, f"/synthetic/{variant}.png"),
            )
        conn.execute(f"PRAGMA user_version={version}")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT subject_detail, subject_detail_px FROM photos").fetchone() == (None, None)
        assert conn.execute("SELECT subject_detail, subject_detail_px FROM photo_masks").fetchall() == [(None, None)] * 2
        assert conn.execute("SELECT COUNT(*) FROM photo_masks WHERE path IS NOT NULL").fetchone()[0] == 2


def test_version_34_catalog_gains_subject_detail_columns(tmp_path):
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        for table in ("photo_masks", "photos"):
            conn.execute(f"ALTER TABLE {table} DROP COLUMN subject_detail")
            conn.execute(f"ALTER TABLE {table} DROP COLUMN subject_detail_px")
        conn.execute("PRAGMA user_version=34")
    schema.ensure_schema(path)
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        for table in ("photo_masks", "photos"):
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            assert {"subject_detail", "subject_detail_px"} <= columns
        # Existing rows are unmeasured, never zero.
        assert conn.execute("SELECT COUNT(*) FROM photos WHERE subject_detail IS NOT NULL").fetchone()[0] == 0


@pytest.mark.parametrize("version", [38, 39, 40])
def test_catalog_without_error_details_preserves_historical_job_on_upgrade(tmp_path, version):
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE job_history DROP COLUMN errors")
        conn.execute(f"PRAGMA user_version={version}")
        conn.execute(
            "INSERT INTO job_history (id, type, status, result, error_count) "
            """VALUES ('old-job', 'scan', 'failed', '{"error":"old failure"}', 1)"""
        )
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT status, result, error_count, errors FROM job_history WHERE id='old-job'"
        ).fetchone()
        assert row == ("failed", '{"error":"old failure"}', 1, None)


def test_alternate_version_39_catalog_gains_job_history_errors(tmp_path):
    """A catalog stamped v39 by this branch before merging main lacks the
    ``errors`` column that main's v39 adds. Migration 48 must repair it so
    ``persist_job`` can still write history after the upgrade."""
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE job_history DROP COLUMN errors")
        conn.execute("PRAGMA user_version=39")
    schema.ensure_schema(path)
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        columns = {row[1] for row in conn.execute("PRAGMA table_info(job_history)")}
        assert "errors" in columns
        conn.execute(
            "INSERT INTO job_history (id, type, status, result, error_count, errors) "
            """VALUES ('new-job', 'scan', 'completed', '{}', 0, '[]')"""
        )
        row = conn.execute(
            "SELECT status, errors FROM job_history WHERE id='new-job'"
        ).fetchone()
        assert row == ("completed", "[]")


def test_primary_policy_migration_preserves_projected_subject(tmp_path):
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE photo_subject_state")
        conn.execute("CREATE TABLE photo_subject_state (photo_id INTEGER PRIMARY KEY, detection_id INTEGER NOT NULL)")
        conn.execute("INSERT INTO photo_subject_state VALUES (10, 20)")
        conn.execute("PRAGMA user_version=38")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT detection_id, min_conf, detector_model, refresh_pending "
                            "FROM photo_subject_state WHERE photo_id=10").fetchone() == (20, None, None, 0)


@pytest.mark.parametrize("version", [39, 40])
def test_alternate_branch_catalog_gains_main_migrations_without_startup_ddl(tmp_path, version):
    path = str(tmp_path / "synthetic.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE job_history DROP COLUMN errors")
        conn.execute("DROP TABLE subject_focus")
        conn.execute("DROP INDEX idx_photos_burst_capture")
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert "errors" in {row[1] for row in conn.execute("PRAGMA table_info(job_history)")}
        assert {row[1] for row in conn.execute("PRAGMA table_info(subject_focus)")} == {
            "detection_id", "af", "sharp", "continuity",
        }
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='idx_photos_burst_capture'").fetchone()


def test_branch_version_45_preserves_ranked_species_highlights(tmp_path):
    path = str(tmp_path / "catalog.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO folders (id, path) VALUES (1, '/synthetic')")
        conn.execute("INSERT INTO photos (id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
        conn.execute("CREATE TABLE species_highlights (workspace_id, species, photo_id, rank, created_at)")
        conn.execute("INSERT INTO species_highlights VALUES (1, 'Mallard', 1, 1, 0)")
        conn.execute("PRAGMA user_version=45")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT species, photo_id FROM species_representatives").fetchall() == [("Mallard", 1)]
        assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name='species_highlights'").fetchone()


@pytest.mark.parametrize("layout", ["main", "classifier_source"])
def test_alternate_version_42_layouts_preserve_data_and_converge(tmp_path, layout):
    path = str(tmp_path / "synthetic-v42.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        folder = conn.execute("INSERT INTO folders(path, name) VALUES ('synthetic', 'synthetic')").lastrowid
        photo = conn.execute("INSERT INTO photos(folder_id, filename) VALUES (?, 'synthetic.jpg')", (folder,)).lastrowid
        detection = conn.execute(
            "INSERT INTO detections(photo_id, detector_model, detector_confidence, category, box_x, box_y, box_w, box_h) "
            "VALUES (?, 'synthetic', .9, 'animal', .1, .1, .2, .2)", (photo,),
        ).lastrowid
        conn.execute(
            "INSERT INTO classifier_runs(detection_id, classifier_model, labels_fingerprint, prediction_count, input_source) "
            "VALUES (?, 'synthetic', 'synthetic', 7, 'working_copy')", (detection,),
        )
        if layout == "main":
            conn.execute("ALTER TABLE classifier_runs DROP COLUMN input_source")
        else:
            conn.execute("DROP TABLE species_choice_winners")
            conn.execute("CREATE TABLE species_highlights (workspace_id INTEGER, species TEXT, photo_id INTEGER, "
                         "rank INTEGER, created_at TEXT)")
            conn.execute("INSERT INTO species_highlights VALUES (1, 'Synthetic species', ?, 1, 'synthetic')", (photo,))
        conn.execute("PRAGMA user_version=42")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute("SELECT prediction_count, input_source FROM classifier_runs WHERE detection_id=?",
                            (detection,)).fetchone() == (7, None if layout == "main" else "working_copy")
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='species_highlights'").fetchone() is None
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='species_choice_winners'").fetchone() is not None
        if layout == "classifier_source":
            assert conn.execute("SELECT photo_id FROM species_representatives WHERE species='Synthetic species'").fetchone() == (photo,)
        assert not list(conn.execute("PRAGMA foreign_key_check"))
        migrated = list(conn.iterdump())
        schema._add_classifier_input_source(conn)
        schema._apply_pending(conn)
        assert list(conn.iterdump()) == migrated


@pytest.mark.parametrize("version", range(43, 48))
def test_branch_catalog_gains_classifier_input_source(tmp_path, version):
    path = str(tmp_path / "catalog.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE classifier_runs DROP COLUMN input_source")
        conn.execute(f"PRAGMA user_version={version}")
        conn.commit()
        schema._apply_pending(conn)
        assert "input_source" in {row[1] for row in conn.execute("PRAGMA table_info(classifier_runs)")}
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


@pytest.mark.parametrize("layout", ["main", "gps_branch"])
def test_version_43_layouts_preserve_reviewed_gps_and_classifier_source(tmp_path, layout):
    path = str(tmp_path / "catalog.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        folder = conn.execute("INSERT INTO folders(path, name) VALUES ('test', 'test')").lastrowid
        photo = conn.execute("INSERT INTO photos(folder_id, filename) VALUES (?, 'bird.jpg')", (folder,)).lastrowid
        if layout == "gps_branch":
            conn.execute("ALTER TABLE classifier_runs DROP COLUMN input_source")
            conn.execute("INSERT INTO reviewed_gps VALUES (?, 12, 34, 'nearby frames')", (photo,))
        else:
            conn.execute("DROP TABLE reviewed_gps")
        conn.execute("PRAGMA user_version=43")

    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert "input_source" in {row[1] for row in conn.execute("PRAGMA table_info(classifier_runs)")}
        expected = [(photo, 12, 34, "nearby frames")] if layout == "gps_branch" else []
        assert conn.execute("SELECT photo_id, latitude, longitude, evidence FROM reviewed_gps").fetchall() == expected


@pytest.mark.parametrize("version", range(44, 52))
def test_branch_versions_reconcile_reviewed_gps(tmp_path, version):
    path = str(tmp_path / "alternate.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE reviewed_gps")
        conn.execute(f"PRAGMA user_version={version}")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM reviewed_gps").fetchone() == (0,)
        assert conn.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)


@pytest.mark.parametrize("lineage", ["main", "detail_branch"])
def test_both_version_44_lineages_converge(tmp_path, lineage):
    """Main's 44 added reviewed_gps; this branch's 44 cleared subject detail instead."""
    path = str(tmp_path / "catalog.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        folder = conn.execute("INSERT INTO folders(path, name) VALUES ('test', 'test')").lastrowid
        photo = conn.execute(
            "INSERT INTO photos(folder_id, filename, subject_detail, subject_detail_px) "
            "VALUES (?, 'bird.jpg', 0.5, 40000)", (folder,)).lastrowid
        if lineage == "main":
            conn.execute("INSERT INTO reviewed_gps VALUES (?, 12, 34, 'nearby frames')", (photo,))
        else:
            conn.execute("DROP TABLE reviewed_gps")
        conn.execute("PRAGMA user_version=44")

    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert conn.execute("SELECT subject_detail, subject_detail_px FROM photos").fetchone() == (None, None)
        expected = [(photo, 12, 34, "nearby frames")] if lineage == "main" else []
        assert conn.execute("SELECT photo_id, latitude, longitude, evidence FROM reviewed_gps").fetchall() == expected


def test_branch_catalog_at_51_gains_reviewed_gps(tmp_path):
    path = str(tmp_path / "catalog.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE reviewed_gps")
        conn.execute("PRAGMA user_version=51")

    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='reviewed_gps'").fetchone()
