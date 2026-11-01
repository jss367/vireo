"""Compatibility contract for storage defaults shared by Python and Tauri."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import app_paths
import pytest
from app_paths import default_path, initialize_layout, media_root

# Deliberately independent of the manifest: changing an existing location must
# be an explicit migration change, not an unnoticed path refactor.
LEGACY_PATHS = {
    "data": ".vireo",
    "log": ".vireo/vireo.log",
    "database": ".vireo/vireo.db",
    "config": ".vireo/config.json",
    "models": ".vireo/models",
    "model_config": ".vireo/models.json",
    "thumbnails": ".vireo/thumbnails",
    "embeddings": ".vireo/embedding_cache",
    "computation_cache": ".vireo/computation-cache",
    "labels": ".vireo/labels",
    "active_labels": ".vireo/labels_active.json",
    "taxonomy_json": ".vireo/taxonomy.json",
    "taxonomy": ".vireo/taxonomy",
    "tools": ".vireo/tools",
    "ssh": ".vireo/ssh",
    "runtime": ".vireo/runtime.json",
    "runtime_lock": ".vireo/runtime.lock",
    "gui_clients": ".vireo/gui-clients",
    "gui_clients_lock": ".vireo/gui-clients.lock",
    "hf_cache_lock": ".vireo/hf-cache-access.lock",
    "desktop_logs_macos": "Library/Logs/com.vireo.app",
}


@pytest.mark.parametrize("name,relative", LEGACY_PATHS.items())
def test_existing_locations_and_home_resolution(name, relative, tmp_path, monkeypatch):
    for home in (tmp_path / "first home", tmp_path / "second home"):
        (home / ".vireo").mkdir(parents=True)
        monkeypatch.setenv("HOME", str(home))
        assert Path(default_path(name)) == home / relative
        assert Path(default_path(name, expand_user=False)) == Path("~") / relative
        assert list(home.iterdir()) == [home / ".vireo"]
        assert not list((home / ".vireo").iterdir())


def test_unknown_name_fails_loudly():
    with pytest.raises(KeyError):
        default_path("missing_name")


def test_cli_and_app_keep_explicit_catalog_and_thumbnail_paths(tmp_path, monkeypatch):
    from app import _new_configured_app, _parse_main_args

    (tmp_path / "home/.vireo").mkdir(parents=True)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setattr(sys, "argv", ["vireo"])
    args = _parse_main_args()
    assert Path(args.db) == tmp_path / "home/.vireo/vireo.db"
    assert Path(args.thumb_dir) == tmp_path / "home/.vireo/thumbnails"

    database = str(tmp_path / "catalog/library.db")
    thumbnails = str(tmp_path / "cache/thumbs")
    monkeypatch.setattr(sys, "argv", ["vireo", "--db", database, "--thumb-dir", thumbnails])
    args = _parse_main_args()
    app = _new_configured_app(args.db, args.thumb_dir, "test-token")
    assert app.config["DB_PATH"] == database
    assert app.config["THUMB_CACHE_DIR"] == thumbnails
    assert app.config["CARD_CLEANUP_DIR"] == str(tmp_path / "catalog/card_cleanup")
    assert Path(app.config["COMPUTATION_CACHE_DIR"]) == tmp_path / "home/.vireo/computation-cache"
    assert not list((tmp_path / "home/.vireo").iterdir())


@pytest.mark.parametrize("legacy", [False, True])
def test_import_time_defaults_in_fresh_process(tmp_path, legacy):
    """Do not reload modules in pytest: other tests patch these public constants."""
    home = tmp_path / "home with spaces"
    if legacy:
        (home / ".vireo").mkdir(parents=True)
    script = '''
import json
import classifier, config, detector, keypoints, labels, models, taxonomy
import text_encoder, thumbnails, timm_classifier
print(json.dumps([
    config.CONFIG_PATH, models.CONFIG_PATH, models.DEFAULT_MODELS_DIR,
    classifier.CACHE_DIR, classifier._MODELS_ROOT, text_encoder._MODELS_ROOT,
    timm_classifier._MODELS_ROOT, keypoints.MODELS_DIR, labels.LABELS_DIR,
    taxonomy.TAXONOMY_JSON_PATH, thumbnails.DEFAULT_CACHE_DIR,
    detector.MEGADETECTOR_ONNX_DIR,
]))
'''
    result = subprocess.run(
        [sys.executable, "-c", script], check=True, capture_output=True, text=True,
        cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, "HOME": str(home), "USERPROFILE": str(home),
             "XDG_DATA_HOME": "", "XDG_CACHE_HOME": "", "LOCALAPPDATA": ""},
    )
    expected = [
        "config.json", "models.json", "models", "embedding_cache", "models",
        "models", "models", "models", "labels", "taxonomy.json", "thumbnails",
        "models/megadetector-v6",
    ]
    data, cache = PLATFORM_ROOTS[sys.platform if sys.platform in PLATFORM_ROOTS else "linux"]
    cache_names = {"models", "embedding_cache", "thumbnails", "models/megadetector-v6"}
    assert [Path(path) for path in json.loads(result.stdout)] == [
        home / (".vireo" if legacy else cache if p in cache_names else data) / p for p in expected
    ]


def test_computation_cache_retains_deferred_home_and_explicit_root(tmp_path, monkeypatch):
    import computation_cache

    monkeypatch.setenv("HOME", str(tmp_path / "new home"))
    assert computation_cache.ArtifactStore().root == Path(default_path("computation_cache"))
    assert str(tmp_path / "new home") in str(computation_cache.ArtifactStore().root)
    custom = tmp_path / "custom results"
    assert computation_cache.ArtifactStore(custom).root == custom


PLATFORM_ROOTS = {
    "darwin": ("Library/Application Support/Vireo", "Library/Caches/Vireo"),
    "win32": ("AppData/Local/Vireo", "AppData/Local/Vireo/Cache"),
    "linux": (".local/share/Vireo", ".cache/Vireo"),
}


@pytest.fixture
def fresh_home(tmp_path, monkeypatch):
    home = tmp_path / "new home"
    monkeypatch.setenv("HOME", str(home))
    for name in ("LOCALAPPDATA", "XDG_DATA_HOME", "XDG_CACHE_HOME"):
        monkeypatch.delenv(name, raising=False)
    return home


@pytest.mark.parametrize("platform", PLATFORM_ROOTS)
def test_fresh_install_defaults_and_persistent_media(platform, fresh_home, monkeypatch):
    monkeypatch.setattr(app_paths, "sys", SimpleNamespace(platform=platform))
    data, cache = (fresh_home / p for p in PLATFORM_ROOTS[platform])
    for name, relative in LEGACY_PATHS.items():
        if name == "desktop_logs_macos":
            assert Path(default_path(name)) == fresh_home / relative
            continue
        root = cache if name in {"models", "thumbnails", "embeddings", "computation_cache"} else data
        suffix = "" if name == "data" else relative.removeprefix(".vireo/")
        assert Path(default_path(name)) == root / suffix
    assert Path(media_root(default_path("thumbnails"))) == data
    assert not fresh_home.exists()


@pytest.mark.parametrize("platform,variable,kind", [
    ("linux", "XDG_DATA_HOME", "data"), ("linux", "XDG_CACHE_HOME", "models"),
    ("win32", "LOCALAPPDATA", "data"),
])
def test_environment_roots(platform, variable, kind, fresh_home, tmp_path, monkeypatch):
    monkeypatch.setattr(app_paths, "sys", SimpleNamespace(platform=platform))
    original = default_path(kind)
    for invalid in ("", "relative/path", "~/not-expanded"):
        monkeypatch.setenv(variable, invalid)
        assert default_path(kind) == original
    redirected = tmp_path / "redirected"
    monkeypatch.setenv(variable, str(redirected))
    expected = redirected / "Vireo" / ("models" if kind == "models" else "")
    assert Path(default_path(kind)) == expected
    assert default_path(kind, expand_user=False) == str(expected)


def test_upgrade_reopens_original_catalog_even_if_native_catalog_exists(fresh_home, monkeypatch):
    from app import _parse_main_args
    from db import Database

    native = Path(default_path("database"))
    native.parent.mkdir(parents=True)
    native.write_bytes(b"unrelated native file")
    old = fresh_home / ".vireo/vireo.db"
    old.parent.mkdir()
    db = Database(str(old))
    workspace = db.create_workspace("Original catalog")
    db.commit()
    db.close()
    monkeypatch.setattr(sys, "argv", ["vireo"])
    args = _parse_main_args()
    assert Path(args.db) == old
    initialize_layout()
    reopened = Database(args.db)
    try:
        assert reopened.workspaces.get(workspace)["name"] == "Original catalog"
    finally:
        reopened.close()
    assert native.read_bytes() == b"unrelated native file"
    assert not (native.parent / "storage-layout-v1").exists()


def test_native_selection_survives_legacy_folder_created_later(fresh_home):
    original = default_path("database")
    initialize_layout()
    (fresh_home / ".vireo").mkdir()
    (fresh_home / ".vireo/vireo.db").write_bytes(b"older app")
    assert default_path("database") == original
    initialize_layout()
    assert (fresh_home / ".vireo/vireo.db").read_bytes() == b"older app"


@pytest.mark.parametrize("dangling", [False, True])
def test_legacy_symlink_keeps_layout(dangling, fresh_home, tmp_path):
    target = tmp_path / "external volume"
    if not dangling:
        target.mkdir()
    fresh_home.mkdir()
    try:
        (fresh_home / ".vireo").symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    assert Path(default_path("database")) == fresh_home / ".vireo/vireo.db"
    initialize_layout()
    assert not (fresh_home / "Library").exists()


def test_unreadable_legacy_directory_never_looks_like_fresh_install(fresh_home, monkeypatch):
    original = app_paths.os.lstat
    def lstat(path, *args, **kwargs):
        if Path(path) == fresh_home / ".vireo":
            raise PermissionError("offline volume")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(app_paths.os, "lstat", lstat)
    assert Path(default_path("database")) == fresh_home / ".vireo/vireo.db"


def test_custom_thumbnail_root_retains_existing_sibling_layout(fresh_home, tmp_path):
    from pipeline_job import _effective_cache_dirs
    custom = str(tmp_path / "custom-cache/thumbs")
    assert media_root(custom) == str(tmp_path / "custom-cache")
    assert _effective_cache_dirs("/catalog/library.db", custom) == (custom, str(tmp_path / "custom-cache"))
    thumb = default_path("thumbnails")
    assert _effective_cache_dirs(default_path("database"), thumb) == (thumb, default_path("data"))
    assert _effective_cache_dirs(default_path("database"), None) == (thumb, default_path("data"))


def test_retained_files_are_outside_native_cache(fresh_home):
    from services.retained_cache import roots
    settings = {"THUMB_CACHE_DIR": default_path("thumbnails"),
                "COMPUTATION_CACHE_DIR": default_path("computation_cache")}
    for name, path in roots(settings).items():
        if name != "computation":
            assert Path(path).is_relative_to(Path(default_path("data")))


def test_test_profiles_reject_native_data_and_cache(fresh_home, monkeypatch):
    from testing.userfirst.profile import UnsafeProfileError, resolve_profile
    for root in app_paths.protected_roots():
        monkeypatch.setenv("VIREO_PROFILE", str(Path(root) / "photos"))
        with pytest.raises(UnsafeProfileError):
            resolve_profile()


def test_native_storage_report_and_safe_cleanup_preserve_media(app_and_db, monkeypatch, tmp_path):
    app, _db = app_and_db
    home = tmp_path / "native home"
    monkeypatch.setenv("HOME", str(home))
    for name in ("LOCALAPPDATA", "XDG_DATA_HOME", "XDG_CACHE_HOME"):
        monkeypatch.delenv(name, raising=False)
    initialize_layout()
    thumbs = Path(default_path("thumbnails"))
    thumbs.mkdir(parents=True)
    thumbnail = thumbs / "1.jpg"
    thumbnail.write_bytes(b"cached thumbnail")
    app.config["THUMB_CACHE_DIR"] = str(thumbs)
    app.config["COMPUTATION_CACHE_DIR"] = default_path("computation_cache")
    data = Path(default_path("data"))
    originals = []
    for directory in ("working", "offline", "staging", "external-edits", "local-workspaces"):
        path = data / directory / "1.jpg"
        path.parent.mkdir()
        path.write_bytes(b"keep photo content")
        originals.append(path)
    preview = data / "previews/1_1600.jpg"
    preview.parent.mkdir()
    preview.write_bytes(b"cached preview")
    client = app.test_client()
    response = client.get("/api/storage")
    assert response.status_code == 200
    report = response.get_json()
    assert report["thumbnails"]["path"] == str(thumbs)
    assert report["working_copies"]["path"] == str(data / "working")
    response = client.post("/api/storage/clear-safe")
    assert response.status_code == 200
    assert not thumbnail.exists()
    assert not preview.exists()
    for path in originals:
        assert path.read_bytes() == b"keep photo content"


@pytest.mark.parametrize("legacy", [False, True, "migrated"])
def test_real_headless_startup_uses_selected_catalog_and_runtime(legacy, fresh_home, tmp_path):
    import time

    from testing.waits import synchronization_timeout

    if legacy:
        (fresh_home / ".vireo").mkdir(parents=True)
    if legacy == "migrated":
        from db import Database
        from services.storage_migration import migrate

        database = Database(str(fresh_home / ".vireo/vireo.db"))
        database.close()
        original = fresh_home / ".vireo/working/unique.raw"
        original.parent.mkdir()
        original.write_bytes(b"unique managed original")
        migrate()
    expected_db = Path(default_path("database"))
    expected_runtime = Path(default_path("runtime"))
    log_path = tmp_path / "startup.log"
    env = {**os.environ, "HOME": str(fresh_home), "USERPROFILE": str(fresh_home),
           "LOCALAPPDATA": "", "XDG_DATA_HOME": "", "XDG_CACHE_HOME": "",
           "VIREO_DISABLE_STARTUP_BACKFILL_TIMERS": "1"}
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve().parents[1] / "app.py"),
             "--headless", "--port", "0"],
            env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + synchronization_timeout(15)
            info = {}
            while time.monotonic() < deadline:
                assert process.poll() is None, log_path.read_text()
                if expected_runtime.exists():
                    info = json.loads(expected_runtime.read_text())
                    if info.get("port") and info.get("token"):
                        break
                time.sleep(0.05)
            assert info.get("port"), log_path.read_text()
            assert Path(info["db_path"]) == expected_db
            assert expected_db.is_file()
            assert expected_runtime.with_suffix(".lock").exists()
            if not legacy:
                assert (expected_db.parent / "storage-layout-v1").exists()
                assert not (fresh_home / ".vireo").exists()
            elif legacy == "migrated":
                assert (expected_db.parent / "storage-layout-v1").exists()
                assert (fresh_home / ".vireo/working/unique.raw").read_bytes() == b"unique managed original"
                assert os.path.samefile(expected_db, fresh_home / ".vireo/vireo.db")
        finally:
            process.terminate()
            try:
                process.wait(timeout=synchronization_timeout(5))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=synchronization_timeout(5))
