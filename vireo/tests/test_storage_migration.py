"""Offline relocation preserves bytes, stored paths, locks, and crash recovery."""

import json
import os
import sqlite3
from pathlib import Path

import pytest
from services import storage_migration as migration


@pytest.fixture
def legacy(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    source, data, cache, journal = migration._paths()
    source.mkdir()
    for name, content in {"models/bird/model.onnx": b"weights", "thumbnails/1.jpg": b"thumbnail",
                          "embedding_cache/labels.npz": b"embeddings", "computation-cache/result": b"result",
                          "working/photo.raw": b"unique original", "offline/photo.raw": b"offline original",
                          "edit-masks/1.png": b"unique mask", "external-editor/edited.tif": b"unique edit",
                          "unknown/future-format": b"preserve unknown data"}.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    with sqlite3.connect(source / "vireo.db") as db:
        db.execute("CREATE TABLE saved (path TEXT)")
        db.execute("INSERT INTO saved VALUES (?)", (str(source / "working/photo.raw"),))
    (source / "config.json").write_text(json.dumps({"custom": str(source / "working")}))
    # Fail before testing migration on Windows without symlink privilege.
    link = source / "link-probe"
    try:
        link.symlink_to(source, target_is_directory=True)
        link.unlink()
    except OSError:
        pytest.skip("Migration requires symlink support")
    return source, data, cache, journal


def snapshot(root):
    return {str(p.relative_to(root)): (p.read_bytes(), p.stat().st_dev, p.stat().st_ino)
            for p in root.rglob("*") if p.is_file()}


def test_preview_is_read_only(legacy):
    source, data, cache, journal = legacy
    before = snapshot(source)
    assert migration.preview()["status"] == "ready"
    assert snapshot(source) == before
    assert not data.exists() and not cache.exists() and not journal.exists()
    assert not (source / "runtime.lock").exists()


def test_migration_preserves_bytes_inodes_saved_paths_and_default_media_root(legacy):
    import app_paths

    source, data, cache, journal = legacy
    before = snapshot(source)
    assert migration.migrate()["status"] == "migrated"
    for relative, expected in before.items():
        target = (cache if Path(relative).parts[0] in migration.CACHES else data) / relative
        assert (target.read_bytes(), target.stat().st_dev, target.stat().st_ino) == expected
        assert (source / relative).read_bytes() == expected[0]
    assert app_paths.default_path("data") == str(data)
    assert app_paths.media_root(app_paths.default_path("thumbnails")) == str(data)
    assert app_paths.default_path("models") == str(cache / "models")
    with sqlite3.connect(data / "vireo.db") as db:
        saved = db.execute("SELECT path FROM saved").fetchone()[0]
        assert saved == str(source / "working/photo.raw")
        assert Path(saved).read_bytes() == b"unique original"
    assert os.path.samefile(data / "runtime.lock", source / "runtime.lock")
    assert os.path.realpath(data / "hf-cache-access.lock") == str(source / "hf-cache-access.lock")
    from services.hf_cache_lock import SharedDownloadLock

    with SharedDownloadLock(str(data / "hf-cache-access.lock")):
        old_lock = SharedDownloadLock(str(source / "hf-cache-access.lock"))
        assert not old_lock.acquire(blocking=False)
    assert not journal.exists()
    assert migration.migrate()["status"] == "native"


@pytest.mark.parametrize("rollback", [False, True])
def test_interrupted_rename_before_link_blocks_startup_and_can_recover(legacy, monkeypatch, rollback):
    import app_paths

    source, data, cache, journal = legacy
    before = snapshot(source)
    real_link = migration._link

    def interrupted(path, target, directory):
        if path.parent == source:
            raise OSError("simulated interruption after rename")
        real_link(path, target, directory)

    with monkeypatch.context() as patch:
        patch.setattr(migration, "_link", interrupted)
        with pytest.raises(OSError, match="interruption"):
            migration.migrate()
    assert journal.exists()
    assert migration.preview()["status"] == "unfinished"
    with pytest.raises(RuntimeError, match="unfinished"):
        app_paths.initialize_layout()
    assert migration.migrate(rollback=rollback)["status"] == ("rolled_back" if rollback else "migrated")
    assert not journal.exists()
    for relative, (content, device, inode) in before.items():
        assert (source / relative).read_bytes() == content
        assert ((source / relative).stat().st_dev, (source / relative).stat().st_ino) == (device, inode)
    assert (data / migration.MARKER).exists() is not rollback


@pytest.mark.parametrize("destination", ["data", "cache"])
def test_destination_collision_does_not_move_or_overwrite_data(legacy, destination):
    source, data, cache, journal = legacy
    root = data if destination == "data" else cache
    root.mkdir(parents=True)
    (root / "vireo.db").write_bytes(b"other installation")
    before = snapshot(source)
    with pytest.raises(migration.MigrationError, match="absent or empty"):
        migration.migrate()
    assert snapshot(source) == before
    assert (root / "vireo.db").read_bytes() == b"other installation"
    assert not journal.exists()


def test_relative_symlink_refused_without_touching_target(legacy):
    source, _data, _cache, journal = legacy
    (source / "relative").symlink_to("working/photo.raw")
    with pytest.raises(migration.MigrationError, match="Relative symlink"):
        migration.migrate()
    assert (source / "relative").read_bytes() == b"unique original"
    assert not journal.exists()


def test_external_absolute_links_and_custom_paths_stay_external(legacy, tmp_path):
    source, data, _cache, _journal = legacy
    outside = tmp_path / "external-original.raw"
    outside.write_bytes(b"outside")
    (source / "linked-original").symlink_to(outside)
    migration.migrate()
    assert (data / "linked-original").read_bytes() == b"outside"
    assert outside.read_bytes() == b"outside"


def test_running_server_lock_refuses_migration(legacy):
    from runtime import _release_lock, _try_take_lock

    source, data, _cache, journal = legacy
    fd = os.open(source / "runtime.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        assert _try_take_lock(fd)
        with pytest.raises(migration.MigrationError, match="Vireo is running"):
            migration.migrate()
        assert not data.exists() and not journal.exists()
    finally:
        _release_lock(fd)
        os.close(fd)


def test_download_lock_refuses_migration(legacy):
    from filelock import FileLock

    source, data, _cache, journal = legacy
    with FileLock(source / "hf-cache-access.lock"):
        with pytest.raises(migration.MigrationError, match="model/cache operation"):
            migration.migrate()
    assert not data.exists() and not journal.exists()


def test_recovery_refuses_replaced_destination(legacy, monkeypatch):
    source, data, cache, journal = legacy

    def interrupted(*_args):
        raise OSError("interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(migration, "_apply", interrupted)
        with pytest.raises(OSError):
            migration.migrate()
    record = json.loads(journal.read_text())
    row = record["entries"][0]
    target = migration._target(row["name"], data, cache)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"do not overwrite")
    with pytest.raises(migration.MigrationError, match="changed"):
        migration.migrate(rollback=True)
    assert target.read_bytes() == b"do not overwrite"
    assert (source / row["name"]).exists()
    assert journal.exists()


def test_nested_windows_cache_layout(legacy, monkeypatch):
    source, _data, _cache, journal = legacy
    data = source.parent / "AppData/Local/Vireo"
    cache = data / "Cache"
    monkeypatch.setattr(migration, "native_roots", lambda: (str(data), str(cache)))
    migration.migrate()
    assert (cache / "models/bird/model.onnx").read_bytes() == b"weights"
    assert (data / "working/photo.raw").read_bytes() == b"unique original"
    assert not journal.exists()


def test_cli_preview_and_recovery_do_not_initialize_native_layout(legacy, monkeypatch, capsys):
    from app import main

    _source, data, _cache, _journal = legacy
    monkeypatch.setattr("sys.argv", ["vireo-server", "--migrate-storage", "preview"])
    with pytest.raises(SystemExit) as exit_info:
        main()
    assert exit_info.value.code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    assert not data.exists()


@pytest.mark.parametrize("move_number", range(1, 12))
@pytest.mark.parametrize("rollback", [False, True])
def test_recovery_after_each_moved_entry(legacy, monkeypatch, move_number, rollback):
    source, data, _cache, journal = legacy
    before = snapshot(source)
    original_rename = Path.rename
    moves = 0

    def interrupted(path, target):
        nonlocal moves
        result = original_rename(path, target)
        if path.parent == source:
            moves += 1
            if moves == move_number:
                raise OSError("interrupted after rename")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, "rename", interrupted)
        with pytest.raises(OSError, match="interrupted"):
            migration.migrate()
    assert journal.exists()
    migration.migrate(rollback=rollback)
    assert not journal.exists()
    assert (data / migration.MARKER).exists() is not rollback
    for relative, (contents, _device, inode) in before.items():
        assert (source / relative).read_bytes() == contents
        assert (source / relative).stat().st_ino == inode


def test_cross_volume_move_is_refused_before_creating_journal(legacy, monkeypatch, tmp_path):
    source, data, _cache, journal = legacy
    other_volume = tmp_path / "other-volume"
    other_volume.mkdir()
    monkeypatch.setattr(migration, "native_roots", lambda: (str(data), str(other_volume / "Vireo")))
    original_stat = Path.stat

    def stat_on_another_device(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == other_volume:
            fields = list(result)
            fields[2] += 1
            return os.stat_result(fields)
        return result

    monkeypatch.setattr(Path, "stat", stat_on_another_device)
    with pytest.raises(migration.MigrationError, match="Cross-volume"):
        migration.migrate()
    assert not journal.exists()
    assert (source / "working/photo.raw").read_bytes() == b"unique original"


def test_missing_or_replaced_root_stops_recovery(legacy, monkeypatch):
    source, data, _cache, journal = legacy
    with monkeypatch.context() as patch:
        patch.setattr(migration, "_apply", lambda *_args: (_ for _ in ()).throw(OSError("stop")))
        with pytest.raises(OSError):
            migration.migrate()
    data.rename(data.with_name("preserved-data"))
    data.mkdir()
    with pytest.raises(migration.MigrationError, match="root changed"):
        migration.migrate(rollback=True)
    assert journal.exists() and (source / "working/photo.raw").exists()
