"""Offline relocation preserves bytes, stored paths, locks, and crash recovery."""

import json
import ntpath
import os
import sqlite3
from contextlib import closing
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace

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
    with closing(sqlite3.connect(source / "vireo.db")) as db, db:
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
    with closing(sqlite3.connect(data / "vireo.db")) as db:
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
    with pytest.raises(migration.MigrationError, match="no unfinished migration"):
        migration.migrate(rollback=True)


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
    (source / "relative").symlink_to(Path("working") / "photo.raw")
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


@pytest.mark.parametrize(("reported", "expected", "matches"), [
    (r"\\?\C:\Users\Vireo\runtime.lock", r"C:\Users\Vireo\runtime.lock", True),
    (r"\\?\C:\Users\Vireo\runtime.lock", "c:/users/vireo/runtime.lock", True),
    (r"\\?\UNC\server\share\models", r"\\server\share\models", True),
    (r"\\?\C:\Users\Vireo\models", r"C:\Users\Vireo\thumbnails", False),
    (r"\\?\UNC\server\share\models", r"\\other\share\models", False),
])
def test_windows_substitution_link_targets_are_compared_lexically(monkeypatch, reported, expected, matches):
    # Use Windows path rules on every host, including case folding and UNC
    # substitution paths. No existing target is required to recognize a link.
    monkeypatch.setattr(migration, "os", SimpleNamespace(name="nt", path=ntpath, readlink=lambda _p: reported))
    link = SimpleNamespace(is_symlink=lambda: True)
    assert migration._is_link(link, PureWindowsPath(expected)) is matches


@pytest.mark.parametrize("rollback", [False, True])
def test_recovery_accepts_extended_readlink_targets(legacy, monkeypatch, rollback):
    source, _data, _cache, journal = legacy

    class SubstitutionPaths:
        name = "nt"

        def __getattr__(self, name):
            return getattr(os, name)

        def readlink(self, path):
            target = os.readlink(path)
            return target if target.startswith("\\\\?\\") else "\\\\?\\" + target

    monkeypatch.setattr(migration, "os", SubstitutionPaths())
    real_link = migration._link

    def interrupted(path, target, directory):
        if path.parent == source:
            raise OSError("simulated interruption")
        real_link(path, target, directory)

    with monkeypatch.context() as patch:
        patch.setattr(migration, "_link", interrupted)
        with pytest.raises(OSError, match="simulated interruption"):
            migration.migrate()
    migration.migrate(rollback=rollback)
    assert not journal.exists()
    assert (source / "working/photo.raw").read_bytes() == b"unique original"


@pytest.mark.skipif(os.name != "posix", reason="POSIX symlink spelling")
def test_posix_link_cannot_impersonate_windows_substitution_target(legacy):
    source, _data, _cache, _journal = legacy
    target = source / "working/photo.raw"
    link = source / "wrong-link"
    link.symlink_to("\\\\?\\" + str(target))
    assert not migration._is_link(link, target)


def test_journal_publication_retries_transient_windows_locks(tmp_path, monkeypatch):
    import file_replace

    journal = tmp_path / "journal.json"
    journal.write_text('{"previous": true}')
    real_replace = os.replace
    calls = []

    def transient_lock(src, dst):
        calls.append((src, dst))
        if len(calls) < 3:
            assert json.loads(journal.read_text()) == {"previous": True}
            raise PermissionError("simulated Windows sharing violation")
        return real_replace(src, dst)

    monkeypatch.setattr(file_replace, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(file_replace.os, "replace", transient_lock)
    monkeypatch.setattr(file_replace.time, "sleep", lambda _delay: None)
    migration._write_journal(journal, {"current": True})
    assert len(calls) == 3
    assert json.loads(journal.read_text()) == {"current": True}
    assert list(tmp_path.iterdir()) == [journal]


def test_target_respects_filesystem_case_sensitivity(tmp_path, monkeypatch):
    """Route a case-variant cache directory to the cache root on case-insensitive filesystems.

    Windows always folds case, and macOS folds case by default. A legacy
    ``Models`` folder on either resolves to the same path as ``models`` and
    must land in the cache root so its installed models stay discoverable
    after the native-layout marker takes effect. On a case-sensitive
    filesystem, ``Models`` is a distinct user-created directory and still
    routes to the data root.
    """
    data, cache = tmp_path / "data", tmp_path / "cache"

    if os.path.normcase("Models") == "Models":
        assert migration._target("Models", data, cache) == data / "Models"
    assert migration._target("models", data, cache) == cache / "models"
    assert migration._target("working", data, cache) == data / "working"

    monkeypatch.setattr(migration.os.path, "normcase", str.lower)
    assert migration._target("Models", data, cache) == cache / "Models"
    assert migration._target("THUMBNAILS", data, cache) == cache / "THUMBNAILS"
    assert migration._target("Embedding_Cache", data, cache) == cache / "Embedding_Cache"
    assert migration._target("working", data, cache) == data / "working"


def test_target_routes_case_variants_on_case_insensitive_filesystem(tmp_path, monkeypatch):
    """A case-insensitive POSIX filesystem routes case variants to the cache root.

    ``os.path.normcase`` only folds case on Windows, so a POSIX host whose
    volume is case-insensitive (macOS APFS/HFS+ by default) needs the
    explicit filesystem probe. Without it, a legacy ``Models`` folder would
    be classified as user data and stranded under the data root after the
    native-layout marker takes effect.
    """
    data, cache = tmp_path / "data", tmp_path / "cache"

    assert migration._is_cache_name("Models", case_insensitive=True)
    assert migration._is_cache_name("THUMBNAILS", case_insensitive=True)
    assert migration._is_cache_name("Embedding_Cache", case_insensitive=True)
    assert not migration._is_cache_name("working", case_insensitive=True)

    assert migration._target("Models", data, cache, case_insensitive=True) == cache / "Models"
    assert migration._target("THUMBNAILS", data, cache, case_insensitive=True) == cache / "THUMBNAILS"
    assert migration._target("working", data, cache, case_insensitive=True) == data / "working"


def test_case_insensitive_probe_detects_folding(tmp_path, monkeypatch):
    """The filesystem probe reports folding by toggling the leaf's casing."""
    probed = tmp_path / "Vireo"
    probed.mkdir()

    def fake_samefile(path, other):
        return str(other) == str(probed.parent / "vIREO")

    monkeypatch.setattr(Path, "samefile", fake_samefile)
    assert migration._case_insensitive(probed) is True

    monkeypatch.setattr(Path, "samefile", lambda _self, _other: False)
    assert migration._case_insensitive(probed) is False

    def raiser(_self, _other):
        raise FileNotFoundError

    monkeypatch.setattr(Path, "samefile", raiser)
    assert migration._case_insensitive(probed) is False

    no_case = tmp_path / "123"
    no_case.mkdir()
    assert migration._case_insensitive(no_case) is False


def test_apply_routes_case_variant_cache_on_case_insensitive_filesystem(legacy, monkeypatch):
    """Case-variant cache directories still land in the cache root on macOS.

    When the source filesystem folds case, a legacy ``Models`` folder is
    the same on-disk directory as ``models`` and must route to the cache
    root. ``os.path.normcase`` alone does not detect macOS folding, so the
    migration asks the filesystem.
    """
    source, data, cache, _journal = legacy
    # Rename `models` to the case-variant the finding covers; preserve the
    # file hierarchy so the migration can move the actual directory.
    (source / "models").rename(source / "Models")
    monkeypatch.setattr(migration, "_case_insensitive", lambda _path: True)

    migration.migrate()

    assert (cache / "Models").is_dir()
    assert (cache / "Models" / "bird" / "model.onnx").read_bytes() == b"weights"
    # The compatibility link lives in the data root under the on-disk name
    # and points at the real cache directory; the model bytes are not left
    # behind as a plain directory under the data root.
    assert (data / "Models").is_symlink()
    assert (data / "Models").resolve() == (cache / "Models").resolve()
    assert (source / "Models").is_symlink()


def test_is_guard_name_respects_case_insensitive_probe():
    """Guard-name membership mirrors the cache-name helper's case rules."""
    assert migration._is_guard_name("runtime.lock")
    assert migration._is_guard_name("hf-cache-access.lock")
    assert not migration._is_guard_name("Runtime.Lock")
    assert migration._is_guard_name("Runtime.Lock", case_insensitive=True)
    assert migration._is_guard_name("HF-Cache-Access.LOCK", case_insensitive=True)
    assert not migration._is_guard_name("other.lock", case_insensitive=True)


def test_record_excludes_case_variant_guards_on_case_insensitive_filesystem(legacy, monkeypatch):
    """A ``Runtime.Lock`` on a case-insensitive volume stays out of the record.

    Recording it as an ordinary entry would collide with the lowercase guard
    symlink ``_apply`` installs under the data root, because both names
    resolve to the same inode on a case-insensitive filesystem.
    """
    source, data, cache, _journal = legacy
    data.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    (source / "Runtime.Lock").write_bytes(b"")
    (source / "HF-Cache-Access.LOCK").write_bytes(b"")
    monkeypatch.setattr(migration, "_case_insensitive", lambda _path: True)

    record = migration._record(source, data, cache)
    names = {row["name"] for row in record["entries"]}

    assert "Runtime.Lock" not in names
    assert "HF-Cache-Access.LOCK" not in names
    # Non-guard case variants still flow through as ordinary entries.
    assert "working" in names


def test_read_journal_rejects_case_variant_guard_entry_on_case_insensitive_filesystem(
        legacy, monkeypatch):
    """Journal validation refuses a case-variant guard masquerading as an entry.

    A tampered or legacy journal that carries a case-variant guard name
    would otherwise let ``_apply`` try to rename the aliased on-disk file
    into its own compatibility symlink. The read-side check must share the
    filesystem probe with the write side.
    """
    source, data, cache, journal = legacy
    data.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    (source / "runtime.lock").write_bytes(b"")
    monkeypatch.setattr(migration, "_case_insensitive", lambda _path: True)
    identity = migration._identity(source / "runtime.lock")
    record = {"version": 1, "source": str(source), "data": str(data), "cache": str(cache),
              "roots": [migration._identity(root) for root in (source, data, cache)],
              "entries": [{"name": "Runtime.Lock", "identity": identity, "directory": False}]}
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(migration.MigrationError, match="Invalid entry in migration journal"):
        migration._read_journal(journal, source, data, cache)
