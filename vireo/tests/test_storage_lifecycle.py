"""Storage ownership, deletion, accounting, and safe retained-file controls."""

import json
import os
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from services import result_files, retained_cache
from services.grouping_history import record_grouping_edit
from wait import wait_for_job_via_client


def _pipeline(path, ids):
    path.write_text(json.dumps({
        "photos": [{"id": pid} for pid in ids],
        "encounters": [{"photo_ids": ids, "bursts": [{"photo_ids": ids}]}],
    }))


def test_photo_delete_updates_every_workspace_and_invalidates_culling(db, tmp_path):
    folder = db.add_folder(str(tmp_path / "photos"))
    photo = db.add_photo(folder, "bird.jpg", ".jpg", 12, 1)
    keep = db.add_photo(folder, "keep.jpg", ".jpg", 13, 1)
    other = db.create_workspace("Other")
    db.add_workspace_folder(other, folder)
    for ws in (db.active_workspace_id, other):
        _pipeline(tmp_path / f"pipeline_results_ws{ws}.json", [photo, keep])
        (tmp_path / f"culling_results_ws{ws}.json").write_text(json.dumps({
            "species_groups": [{"scene_groups": [{"photos": [{"photo_id": photo}]}]}],
        }))
    unrelated = tmp_path / "culling_results_ws999.json"
    unrelated.write_text(json.dumps({"photos": [{"photo_id": keep}]}))
    db.delete_photos([photo])
    for ws in (db.active_workspace_id, other):
        results = json.loads((tmp_path / f"pipeline_results_ws{ws}.json").read_text())
        assert [p["id"] for p in results["photos"]] == [keep]
        assert results["encounters"][0]["bursts"][0]["photo_ids"] == [keep]
        assert not (tmp_path / f"culling_results_ws{ws}.json").exists()
    assert unrelated.exists()


def test_rollback_does_not_prune_shared_results(db, tmp_path):
    folder = db.add_folder(str(tmp_path / "photos"))
    photo = db.add_photo(folder, "bird.jpg", ".jpg", 12, 1)
    path = tmp_path / f"pipeline_results_ws{db.active_workspace_id}.json"
    _pipeline(path, [photo])
    original = path.read_bytes()
    db.delete_photos([photo], commit=False)
    db.rollback()
    assert db.photos.get(photo)
    assert path.read_bytes() == original


@pytest.mark.parametrize("existing", [True, False])
def test_result_pruning_waits_for_workspace_writer_and_keeps_its_new_results(tmp_path, existing):
    from pipeline_locks import acquire_workspace_regroup
    from testing.waits import synchronization_timeout

    path = tmp_path / "pipeline_results_ws99.json"
    if existing:
        _pipeline(path, [1, 2])
    started, finished = threading.Event(), threading.Event()
    def prune():
        started.set()
        result_files.prune_photo_results(str(tmp_path), [1], [99])
        finished.set()
    with acquire_workspace_regroup(99):
        worker = threading.Thread(target=prune)
        worker.start()
        assert started.wait(synchronization_timeout(5))
        assert not finished.wait(0.05)
        _pipeline(path, [1, 2, 3])
    worker.join(synchronization_timeout(5))
    assert finished.is_set()
    assert [photo["id"] for photo in json.loads(path.read_text())["photos"]] == [2, 3]


def test_workspace_delete_removes_snapshots_history_and_never_reuses_id(app_and_db, tmp_path):
    _app, db = app_and_db
    ws = db.create_workspace("Delete me")
    pipeline = tmp_path / f"pipeline_results_ws{ws}.json"
    culling = tmp_path / f"culling_results_ws{ws}.json"
    pipeline.write_text("{}")
    culling.write_text("{}")
    db.conn.execute("INSERT INTO job_history (id, workspace_id) VALUES ('old-job', ?)", (ws,))
    db.conn.commit()
    db.delete_workspace(ws)
    assert not pipeline.exists() and not culling.exists()
    assert db.job_history.get('old-job') is None
    replacement = db.create_workspace("Replacement")
    assert replacement > ws
    # A model download may persist its final state after the owner was deleted.
    db.conn.execute("INSERT OR REPLACE INTO job_history (id, workspace_id) VALUES ('late-job', ?)", (ws,))
    db.conn.commit()
    assert db.job_history.get('late-job') is None


@pytest.mark.parametrize("job_type", ["cull", "regroup", "pipeline"])
def test_workspace_delete_rejects_inactive_workspace_with_result_publisher(app_and_db, tmp_path, job_type):
    from testing.waits import synchronization_timeout

    app, db = app_and_db
    workspace = db.create_workspace("Publishing results")
    assert workspace != db.active_workspace_id
    started, resume = threading.Event(), threading.Event()
    path = tmp_path / f"culling_results_ws{workspace}.json"
    def publish(_job):
        started.set()
        assert resume.wait(synchronization_timeout(5))
        path.write_text("{}")
    job_id = app._job_runner.start(job_type, publish, workspace_id=workspace)
    client = app.test_client()
    try:
        assert started.wait(synchronization_timeout(5))
        response = client.delete(f'/api/workspaces/{workspace}')
        assert response.status_code == 409
        assert db.workspaces.get(workspace)
    finally:
        resume.set()
    wait_for_job_via_client(client, job_id, wait_for_history=True)
    assert path.exists()
    assert client.delete(f'/api/workspaces/{workspace}').status_code == 200
    assert not path.exists()


def test_locked_workspace_snapshot_does_not_belong_to_new_workspace(db, tmp_path, monkeypatch):
    ws = db.create_workspace("Delete me")
    path = tmp_path / f"pipeline_results_ws{ws}.json"
    path.write_text("{}")
    real_unlink = os.unlink

    def unlink(target, *args, **kwargs):
        if str(target) == str(path):
            raise PermissionError("locked")
        return real_unlink(target, *args, **kwargs)

    monkeypatch.setattr(result_files.os, "unlink", unlink)
    errors = db.delete_workspace(ws)
    assert len(errors) == 1 and "locked" in errors[0]
    assert path.exists()
    assert db.create_workspace("Replacement") > ws


def test_legacy_orphan_result_id_is_reserved(db, tmp_path):
    (tmp_path / "pipeline_results_ws500.json").write_text("{}")
    assert db.create_workspace("New workspace") > 500


def test_delete_cleans_catalog_masks_with_separate_thumbnail_root(app_and_db, tmp_path):
    app, db = app_and_db
    photo = db.photos.list_page()[0]["id"]
    thumbs = tmp_path / "separate-volume" / "thumbs"
    thumbs.mkdir(parents=True)
    app.config["THUMB_CACHE_DIR"] = str(thumbs)
    masks = tmp_path / "masks"
    masks.mkdir()
    mask = masks / f"{photo}.sam2-small.png"
    mask.write_bytes(b"mask")
    sentinel = masks / "999.sam2-small.png"
    sentinel.write_bytes(b"other")
    db.conn.execute(
        "INSERT INTO photo_masks (photo_id, variant, path, created_at, detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', ?, 1, 'test', 0, 0, 1, 1)", (photo, str(mask)),
    )
    db.conn.commit()
    response = app.test_client().post('/api/batch/delete', json={"photo_ids": [photo], "mode": "vireo"})
    assert response.status_code == 200
    assert not mask.exists()
    assert sentinel.exists()
    assert not db.conn.execute("SELECT 1 FROM photo_masks WHERE photo_id=?", (photo,)).fetchone()


@pytest.mark.parametrize("indexed", [False, True])
def test_recycled_id_mask_purge_uses_catalog_root_with_custom_thumbnails(db, tmp_path, indexed):
    from preview_cache import RecycledIdIndex, purge_cached_files_for_recycled_id

    thumbs = tmp_path / "other-volume" / "thumbs"
    thumbs.mkdir(parents=True)
    mask = _payload(tmp_path / "masks" / "123.png")
    index = RecycledIdIndex(str(thumbs), masks_dir=str(mask.parent)) if indexed else None
    assert purge_cached_files_for_recycled_id(str(thumbs), 123, db=db, id_index=index)
    assert not mask.exists()


def test_memory_catalog_never_purges_masks_relative_to_process_directory(tmp_path, monkeypatch):
    from db import Database
    from preview_cache import purge_cached_files_for_recycled_id

    monkeypatch.chdir(tmp_path)
    unrelated = _payload(tmp_path / "masks" / "123.png")
    with Database(":memory:") as db:
        assert not purge_cached_files_for_recycled_id(str(tmp_path / "generated" / "thumbs"), 123, db=db)
    assert unrelated.exists()


def _cache_config(tmp_path):
    return {"THUMB_CACHE_DIR": str(tmp_path / "generated" / "thumbs"),
            "COMPUTATION_CACHE_DIR": str(tmp_path / "computations"),
            "retained_cache_max_mb": 0, "retained_cache_max_age_days": 30}


def _payload(path, content=b"payload", when=100):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    os.utime(path, (when, when))
    return path


def test_automatic_retention_protects_recent_writes_and_important_files(tmp_path):
    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent
    old = _payload(root / "originals" / "1.display.jpg")
    now = time.time() + retained_cache.WRITE_GRACE_SECONDS * 2
    fresh = _payload(root / "originals" / "2.jpg", when=now - 100)
    staging = _payload(root / "staging" / "photo.NEF")
    offline = _payload(root / "offline" / "originals" / "1.NEF")
    mask = _payload(root / "edit-masks" / "1.0123456789ab.png")
    trust = _payload(Path(config["COMPUTATION_CACHE_DIR"]) / "trust.json")
    object_path = _payload(Path(config["COMPUTATION_CACHE_DIR"]) / "objects" / "abc" / ("a" * 64 + ".json"))
    result = retained_cache.cleanup(config, [], now=now, source_is_readable=lambda _path: True)
    assert result["deleted"] == 2 and result["ok"]
    assert not old.exists() and not object_path.exists()
    assert all(p.exists() for p in (fresh, staging, offline, mask, trust))


def test_automatic_retention_protects_new_renders_with_old_source_timestamps(tmp_path):
    config = _cache_config(tmp_path)
    path = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "originals" / "1.jpg", when=1)
    result = retained_cache.cleanup(config, [], now=time.time())
    assert result["deleted"] == 0
    assert path.exists()


def test_retention_preserves_external_editor_changes_under_age_and_quota_limits(tmp_path):
    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent
    edited = _payload(root / "external-edits" / "1.jpg", b"unique external edits")
    metadata = _payload(root / "external-edits" / "1.json")
    converted = _payload(root / "external-dng" / "1" / "bird.dng", b"edited conversion")
    retained_cache.cleanup(config, [], now=time.time() + 365 * 86400)
    assert edited.read_bytes() == b"unique external edits"
    assert metadata.exists() and converted.read_bytes() == b"edited conversion"
    for family in ("external-editor", "external-dng"):
        with pytest.raises(ValueError, match="unique edits"):
            retained_cache.cleanup(config, [], family=family, clear=True)


@pytest.mark.parametrize("problem", [None, "offline", "unreadable", "changed", "hash", "no-hash", "cache-source"])
def test_automatic_render_retention_requires_independent_readable_sources(db, tmp_path, monkeypatch, problem):
    import builtins
    import hashlib

    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent
    source = _payload((root / "offline" if problem == "cache-source" else tmp_path / "photos") / "bird.jpg")
    info = source.stat()
    folder = db.add_folder(str(source.parent))
    photo = db.add_photo(folder, source.name, ".jpg", info.st_size, info.st_mtime)
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", (hashlib.sha256(source.read_bytes()).hexdigest(), photo))
    db.commit()
    render = _payload(root / "originals" / f"{photo}.display.jpg")
    if problem == "offline":
        source.unlink()
    elif problem == "changed":
        source.write_bytes(b"changed")
    elif problem == "hash":
        source.write_bytes(b"xxxxxxx")
        os.utime(source, (info.st_atime, info.st_mtime))
    elif problem == "no-hash":
        db.conn.execute("UPDATE photos SET file_hash=NULL WHERE id=?", (photo,))
        db.commit()
    elif problem == "unreadable":
        real_open = builtins.open
        def refuse(path, *args, **kwargs):
            if str(path) == str(source):
                raise PermissionError("offline volume")
            return real_open(path, *args, **kwargs)
        monkeypatch.setattr(builtins, "open", refuse)
    probe = retained_cache.source_verifier(db, config)
    result = retained_cache.cleanup(
        config, db.storage.source_folder_paths(), now=time.time() + 365 * 86400,
        source_is_readable=probe,
    )
    # Pytest truncates dictionary assertion messages at CI verbosity.
    diagnostics = json.dumps({**result, "source_verification_failure": probe.verification_failure}, sort_keys=True)
    assert result["ok"], diagnostics
    assert result["kept_unverified_sources"] == (1 if problem else 0), diagnostics
    assert bool(probe.verification_failure) is (problem is not None), diagnostics
    assert result["deleted"] == (0 if problem else 1), diagnostics
    assert render.exists() is (problem is not None), diagnostics


@pytest.mark.parametrize("problem", [None, "changed", "hash", "missing-identity", "missing-hash", "stale-path"])
def test_automatic_paired_render_retention_verifies_stored_companion_identity(db, tmp_path, problem):
    import hashlib

    config = _cache_config(tmp_path)
    raw = _payload(tmp_path / "photos" / "bird.NEF")
    companion = _payload(raw.parent / "bird.jpg")
    folder = db.add_folder(str(raw.parent))
    photo = db.add_photo(folder, raw.name, ".nef", raw.stat().st_size, raw.stat().st_mtime)
    db.conn.execute("UPDATE photos SET file_hash=?, companion_path=? WHERE id=?", (
        hashlib.sha256(raw.read_bytes()).hexdigest(), companion.name, photo,
    ))
    if problem != "missing-identity":
        db.conn.execute(
            "INSERT INTO companion_identities(photo_id, filename, file_size, file_mtime, file_hash) VALUES (?,?,?,?,?)",
            (photo, "old.jpg" if problem == "stale-path" else companion.name,
             companion.stat().st_size, companion.stat().st_mtime,
             None if problem == "missing-hash" else hashlib.sha256(companion.read_bytes()).hexdigest()),
        )
    db.commit()
    if problem == "changed":
        companion.write_bytes(b"changed companion")
    elif problem == "hash":
        companion.write_bytes(b"xxxxxxx")
        os.utime(companion, (100, 100))
    render = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "originals" / f"{photo}.display.jpg")
    probe = retained_cache.source_verifier(db, config)
    result = retained_cache.cleanup(
        config, db.storage.source_folder_paths(), now=time.time() + 365 * 86400,
        source_is_readable=probe,
    )
    diagnostics = json.dumps({**result, "source_verification_failure": probe.verification_failure}, sort_keys=True)
    assert result["ok"], diagnostics
    assert result["kept_unverified_sources"] == (1 if problem else 0), diagnostics
    assert bool(probe.verification_failure) is (problem is not None), diagnostics
    assert result["deleted"] == (0 if problem else 1), diagnostics
    assert render.exists() is (problem is not None), diagnostics


def test_prepared_render_reader_is_pinned_until_response_opens(app_and_db, tmp_path, monkeypatch):
    from testing.waits import synchronization_timeout
    from web import media

    app, db = app_and_db
    config = _cache_config(tmp_path)
    render = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "originals" / "1.jpg")
    checked, resume, cleanup_started = threading.Event(), threading.Event(), threading.Event()
    responses, results = [], []
    def prepared(*_args):
        assert render.exists()
        checked.set()
        assert resume.wait(synchronization_timeout(5))
        return str(render)
    monkeypatch.setattr(media, "_prepared_full_resolution_render", prepared)
    photo = db.photos.list_page()[0]["id"]
    def read():
        with app.test_client() as client:
            response = client.get(f'/photos/{photo}/original')
            responses.append((response.status_code, response.data))
            response.close()
    def clear():
        cleanup_started.set()
        results.append(retained_cache.cleanup(config, [], family="full-resolution", clear=True))
    reader = threading.Thread(target=read)
    cleaner = threading.Thread(target=clear)
    reader.start()
    try:
        assert checked.wait(synchronization_timeout(5))
        cleaner.start()
        assert cleanup_started.wait(synchronization_timeout(5))
        cleaner.join(0.05)
        assert cleaner.is_alive() and render.exists()
    finally:
        resume.set()
        reader.join(synchronization_timeout(5))
        if cleaner.ident:
            cleaner.join(synchronization_timeout(5))
    assert responses == [(200, b"payload")]
    assert len(results) == 1
    # Windows may retain a file while the response descriptor is open.
    retained_cache.cleanup(config, [], family="full-resolution", clear=True)
    assert not render.exists()


def test_paired_render_publication_waits_until_eviction_finishes(app_and_db, tmp_path, monkeypatch):
    from types import SimpleNamespace

    import image_loader
    from PIL import Image
    from testing.waits import synchronization_timeout
    from web import media

    app, _db = app_and_db
    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent
    path = Path(media._paired_original_path(str(root), 1, "raw", "state"))
    _payload(path, b"old rendition")
    before_unlink, resume, started = threading.Event(), threading.Event(), threading.Event()
    responses, results = [], []
    unlink = os.unlink
    def pause_unlink(target, *args, **kwargs):
        if str(target) == str(path):
            before_unlink.set()
            assert resume.wait(synchronization_timeout(5))
        return unlink(target, *args, **kwargs)
    monkeypatch.setattr(retained_cache.os, "unlink", pause_unlink)
    monkeypatch.setattr(image_loader, "load_image", lambda *_args, **_kwargs: Image.new("RGB", (8, 8)))
    def clear():
        results.append(retained_cache.cleanup(config, [], family="full-resolution", clear=True))
    def publish():
        started.set()
        with app.app_context():
            render = SimpleNamespace(vireo_dir=str(root), photo_id=1, recipe=None)
            response = media._OriginalPhotoRequest._render_paired_original(render, "raw", "synthetic-source", str(path))
            responses.append(response.get_data())
    cleaner, publisher = threading.Thread(target=clear), threading.Thread(target=publish)
    cleaner.start()
    try:
        assert before_unlink.wait(synchronization_timeout(5))
        publisher.start()
        assert started.wait(synchronization_timeout(5))
        publisher.join(0.05)
        assert publisher.is_alive()
    finally:
        resume.set()
        cleaner.join(synchronization_timeout(5))
        if publisher.ident:
            publisher.join(synchronization_timeout(5))
    assert results[0]["deleted"] == 1
    assert len(responses) == 1 and path.read_bytes() == responses[0]


def test_source_verification_preserves_change_time_with_windows_stat_semantics(db, tmp_path, monkeypatch):
    import hashlib
    from types import SimpleNamespace

    config = _cache_config(tmp_path)
    source = _payload(tmp_path / "source" / "bird.jpg")
    info = source.stat()
    folder = db.add_folder(str(source.parent))
    photo = db.add_photo(folder, source.name, ".jpg", info.st_size, info.st_mtime)
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", (hashlib.sha256(source.read_bytes()).hexdigest(), photo))
    db.commit()
    render = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "inat-uploads" / f"{photo}.jpg")
    version = [0]

    def descriptor_stat(fd):
        actual = os.fstat(fd)
        fields = {name: getattr(actual, name) for name in dir(actual) if name.startswith("st_")}
        # CPython's Windows path stat exposes creation time as ctime,
        # while fstat exposes ChangeTime. They need not be equal.
        fields["st_birthtime_ns"] = info.st_ctime_ns
        fields["st_birthtime"] = info.st_ctime_ns / 1e9
        fields["st_ctime_ns"] = info.st_ctime_ns + 1_000_000 + version[0]
        fields["st_ctime"] = fields["st_ctime_ns"] / 1e9
        return SimpleNamespace(**fields)

    windows_os = SimpleNamespace(**vars(os))
    windows_os.name = "nt"
    windows_os.fstat = descriptor_stat
    # Replace only this module's OS view, not global os.name/pathlib.
    monkeypatch.setattr(retained_cache, "os", windows_os)
    probe = retained_cache.source_verifier(db, config)
    assert probe(str(render)), json.dumps(probe.verification_failure, sort_keys=True)

    source.write_bytes(b"xxxxxxx")
    os.utime(source, (info.st_atime, info.st_mtime))
    version[0] += 1_000_000
    assert not probe.recheck(str(render))


@pytest.mark.parametrize("change", [None, "offline", "changed", "restored-mtime", "replaced"])
def test_retention_rechecks_source_after_waiting_for_reader_guard(db, tmp_path, monkeypatch, change):
    import hashlib

    from testing.waits import synchronization_timeout

    config = _cache_config(tmp_path)
    source = _payload(tmp_path / "source" / "bird.jpg")
    info = source.stat()
    folder = db.add_folder(str(source.parent))
    photo = db.add_photo(folder, source.name, ".jpg", info.st_size, info.st_mtime)
    db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", (hashlib.sha256(source.read_bytes()).hexdigest(), photo))
    db.commit()
    render = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "inat-uploads" / f"{photo}.jpg")
    verified, waiting, results = threading.Event(), threading.Event(), []
    verification = []
    verification_failures = []
    guard = retained_cache.reader_guard("inat-uploads")

    class ObservedGuard:
        def __enter__(self):
            # Prove contention before signalling: an uncontended schedule
            # would exercise ordering of operations, not the reader-guard
            # block this regression is named for.
            assert not guard.acquire(blocking=False)
            waiting.set()
            return guard.__enter__()

        def __exit__(self, *args):
            return guard.__exit__(*args)

    monkeypatch.setattr(retained_cache, "reader_guard", lambda family: ObservedGuard())

    def maintain():
        from db import Database
        with Database(db._db_path) as connection:
            probe = retained_cache.source_verifier(connection, config)
            class ObservedProbe:
                recheck = staticmethod(probe.recheck)
                def __call__(self, path):
                    result = probe(path)
                    verification.append(result)
                    if not result:
                        verification_failures.append(dict(probe.verification_failure))
                    verified.set()
                    return result
            results.append(retained_cache.cleanup(
                config, [], now=time.time() + 365 * 86400, source_is_readable=ObservedProbe(),
            ))

    def source_diagnostics():
        return json.dumps({
            "verification": verification, "results": results,
            "verification_failures": verification_failures,
        }, sort_keys=True)

    worker = threading.Thread(target=maintain)
    try:
        with guard:
            worker.start()
            assert verified.wait(synchronization_timeout(5)), source_diagnostics()
            if verification == [True]:
                assert waiting.wait(synchronization_timeout(5)), source_diagnostics()
                if change == "offline":
                    source.unlink()
                elif change == "changed":
                    source.write_bytes(b"modified while an upload was using its handoff")
                elif change == "restored-mtime":
                    source.write_bytes(b"xxxxxxx")
                    os.utime(source, (info.st_atime, info.st_mtime))
                elif change == "replaced":
                    replacement = _payload(source.with_name("replacement.jpg"), when=info.st_mtime)
                    os.replace(replacement, source)
    finally:
        worker.join(synchronization_timeout(5))
    assert not worker.is_alive()
    assert verification == [True], source_diagnostics()
    assert len(results) == 1
    result = results[0]
    diagnostics = json.dumps(result, sort_keys=True)
    assert result["ok"], diagnostics
    assert result["kept_unverified_sources"] == (1 if change else 0), diagnostics
    assert result["kept_changed_files"] == 0, diagnostics
    assert result["kept_accessed_files"] == 0, diagnostics
    assert result["kept_unsafe_paths"] == 0, diagnostics
    assert result["deleted"] == (0 if change else 1), diagnostics
    assert render.exists() is (change is not None), diagnostics


def test_retained_cleanup_rejects_photo_folders_and_symlinks(tmp_path):
    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent
    source = _payload(root / "originals" / "1.jpg")
    other = _payload(tmp_path / "source" / "2.dng")
    link = root / "inat-uploads"
    try:
        link.symlink_to(other.parent, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks unavailable")
    result = retained_cache.cleanup(config, [str(source.parent)], clear=True)
    assert not result["ok"] and len(result["errors"]) == 2
    assert source.exists() and other.exists()


@pytest.mark.parametrize("relation", ["parent", "same", "child"])
def test_retained_cleanup_rejects_photo_folder_overlap_in_both_directions(tmp_path, relation):
    config = _cache_config(tmp_path)
    root = Path(config["THUMB_CACHE_DIR"]).parent / "originals"
    original = _payload(root / "1.jpg", b"catalog original")
    folder = root.parent if relation == "parent" else root / "child" if relation == "child" else root
    result = retained_cache.cleanup(config, [str(folder)], family="full-resolution", clear=True)
    assert result["deleted"] == 0 and not result["ok"]
    assert original.read_bytes() == b"catalog original"
    assert result["errors_total"] == 1


def test_failed_unlink_remains_counted(tmp_path, monkeypatch):
    config = _cache_config(tmp_path)
    path = _payload(Path(config["THUMB_CACHE_DIR"]).parent / "originals" / "1.jpg")
    real_unlink = os.unlink

    def unlink(target, *args, **kwargs):
        if str(target) == str(path):
            raise PermissionError("locked")
        return real_unlink(target, *args, **kwargs)

    monkeypatch.setattr(retained_cache.os, "unlink", unlink)
    result = retained_cache.cleanup(config, [], family="full-resolution", clear=True)
    assert not result["ok"] and result["deleted"] == 0
    measured = retained_cache.summary(config)
    assert measured[0]["size"] == len(b"payload")


def _grouping_edit(db, *, photo_edit=None, undone=False):
    change = {"before": {"encounters": []}, "after": {"encounters": [], "padding": "x" * 100}}
    if photo_edit:
        change["photo_edit"] = photo_edit
    edit = record_grouping_edit(db, "Grouped photos", change)
    db.conn.execute("UPDATE edit_history SET undone=? WHERE id=?", (undone, edit))
    db.commit()
    return edit


def test_grouping_budget_preserves_photo_edits_and_redo(db):
    mixed = _grouping_edit(db, photo_edit={"action_type": "flag", "new_value": "flagged"})
    pure = _grouping_edit(db)
    redo = _grouping_edit(db, undone=True)
    db.edit_history.prune(1000, max_payload_bytes=0)
    assert db.edit_history.grouping_payload(redo)
    assert not db.edit_history.grouping_payload(mixed)
    assert not db.edit_history.grouping_payload(pure)
    row = db.conn.execute("SELECT new_value FROM edit_history WHERE id=?", (mixed,)).fetchone()
    assert json.loads(row[0])["photo_only"] is True
    assert not db.conn.execute("SELECT 1 FROM edit_history WHERE id=?", (pure,)).fetchone()


def test_grouping_accounting_and_pruning_do_not_read_large_snapshot_text(db):
    edit = _grouping_edit(db)
    db.edit_history.store_grouping_payload(edit, "\U0001f985")
    db.commit()

    def authorize(action, table, column, *_args):
        if action == sqlite3.SQLITE_READ and table == 'edit_history_payloads' and column == 'payload':
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    db.conn.set_authorizer(authorize)
    try:
        assert db.storage.grouping_history()["bytes"] == 4
        db.edit_history.prune(1000, max_payload_bytes=0)
        assert db.storage.grouping_history()["bytes"] == 0
    finally:
        db.conn.set_authorizer(None)


def test_grouping_retention_rolls_back_partial_retirement(db, monkeypatch):
    from repositories.edit_history import EditHistoryRepository

    first, second = _grouping_edit(db), _grouping_edit(db)
    retire = EditHistoryRepository.retire_stale_grouping_entry
    calls = []

    def fail_second(repository, edit_id):
        calls.append(edit_id)
        if len(calls) == 2:
            raise RuntimeError("interrupted")
        return retire(repository, edit_id)

    monkeypatch.setattr(EditHistoryRepository, "retire_stale_grouping_entry", fail_second)
    with pytest.raises(RuntimeError, match="interrupted"):
        db.edit_history.prune(1000, max_payload_bytes=0)
    assert not db.conn.in_transaction
    assert db.edit_history.grouping_payload(first)
    assert db.edit_history.grouping_payload(second)


def test_grouping_retention_preserves_caller_transaction_and_contiguous_undo(db):
    smaller, oversized = _grouping_edit(db), _grouping_edit(db)
    db.edit_history.store_grouping_payload(smaller, "x" * 10)
    db.edit_history.store_grouping_payload(oversized, "x" * 1000)
    db.commit()
    db.begin_immediate()
    db.edit_history.prune(1000, max_payload_bytes=100)
    assert db.conn.in_transaction
    assert not db.edit_history.grouping_payload(smaller)
    assert not db.edit_history.grouping_payload(oversized)
    db.rollback()
    assert db.edit_history.grouping_payload(oversized)
    assert db.edit_history.grouping_payload(smaller)


def test_maintenance_keeps_workspace_created_during_the_pass(app_and_db, tmp_path, monkeypatch):
    from db import Database
    from services.startup_tasks import StartupTasks

    app, db = app_and_db
    prune = Database._prune_edit_history
    created = []

    def create_during_prune(connection):
        result = prune(connection)
        if not created:
            workspace = db.create_workspace("Created during maintenance")
            path = tmp_path / f"pipeline_results_ws{workspace}.json"
            path.write_text("{}")
            created.append(path)
        return result

    monkeypatch.setattr(Database, "_prune_edit_history", create_during_prune)
    monkeypatch.setattr(app._job_runner, "is_cancelled", lambda _id: False)
    orphan = tmp_path / "culling_results_ws999.json"
    orphan.write_text("{}")
    StartupTasks(app, db._db_path, db)._maintain_retained_caches({"id": "test"})
    assert created[0].exists()
    assert not orphan.exists()


def test_scheduled_maintenance_launches_and_joins_real_singleton_job(app_and_db, monkeypatch):
    from services import startup_tasks
    from testing.waits import synchronization_timeout

    app, db = app_and_db
    timers = []
    class DeferredTimer:
        def __init__(self, delay, callback):
            self.delay, self.callback = delay, callback
        def start(self):
            timers.append(self)
        def cancel(self):
            pass
    monkeypatch.setattr(startup_tasks.threading, "Timer", DeferredTimer)
    tasks = startup_tasks.StartupTasks(app, db._db_path, db)
    started, resume = threading.Event(), threading.Event()
    def work(_job):
        started.set()
        assert resume.wait(synchronization_timeout(5))
        return {"ok": True}
    monkeypatch.setattr(tasks, "_maintain_retained_caches", work)
    try:
        tasks.schedule_retained_cache_maintenance()
        assert timers[0].delay == 10
        timers[0].callback()
        assert started.wait(synchronization_timeout(5))
        assert timers[1].delay == 3600
        timers[1].callback()
        jobs = [job for job in app._job_runner.list_jobs() if job["type"] == "retained-cache-maintenance"]
        assert len(jobs) == 1 and jobs[0]["workspace_id"] is None
        assert len(timers) == 3
        tasks._retained_cache_stopped.set()
        timers[2].callback()
        assert len(timers) == 3
    finally:
        tasks._retained_cache_stopped.set()
        resume.set()
    job = wait_for_job_via_client(app.test_client(), jobs[0]["id"], wait_for_history=True)
    assert job["status"] == "completed"


def test_storage_clear_runs_in_job_and_preserves_computation_trust(app_and_db, tmp_path):
    app, _db = app_and_db
    object_root = tmp_path / "computation-cache"
    app.config["COMPUTATION_CACHE_DIR"] = str(object_root)
    payload = _payload(object_root / "objects" / ("b" * 64 + ".json"))
    trust = _payload(object_root / "trust.json")
    client = app.test_client()
    listed = client.get('/api/storage/retained').get_json()
    assert next(c for c in listed["caches"] if c["type"] == "computation")["size"] == 7
    response = client.post('/api/storage/retained/clear', json={"type": "computation"})
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.get_json()["job_id"], wait_for_history=True)
    assert job["status"] == "completed"
    assert not payload.exists() and trust.exists()
    assert client.post('/api/storage/retained/clear', json={"type": "../staging"}).status_code == 400
    assert client.post('/api/storage/retained/clear', json={"type": "grouping-history"}).status_code == 400


def test_clear_grouping_history_is_explicit_and_keeps_photo_edits(app_and_db):
    app, db = app_and_db
    edit = _grouping_edit(db, photo_edit={"action_type": "flag", "new_value": "flagged"})
    _grouping_edit(db, undone=True)
    client = app.test_client()
    response = client.post('/api/storage/retained/clear', json={"type": "grouping-history", "confirm_history_loss": True})
    job = wait_for_job_via_client(client, response.get_json()["job_id"])
    assert job["status"] == "completed"
    assert db.storage.grouping_history()["bytes"] == 0
    assert db.conn.execute("SELECT 1 FROM edit_history WHERE id=?", (edit,)).fetchone()


def test_split_layout_accounts_for_owned_paths_and_configured_huggingface_cache(app_and_db, tmp_path, monkeypatch):
    app, _db = app_and_db
    import classifier
    from services.storage_paths import huggingface_cache_dir

    generated = tmp_path / "other-volume"
    (generated / "thumbs").mkdir(parents=True)
    app.config["THUMB_CACHE_DIR"] = str(generated / "thumbs")
    hub = tmp_path / "download-volume" / "hub"
    monkeypatch.setenv("HF_HUB_CACHE", str(hub))
    assert huggingface_cache_dir() == str(hub)
    client = app.test_client()
    before = client.get('/api/storage').get_json()
    payloads = [
        _payload(tmp_path / "pipeline_results_ws999.json", b"pipeline"),
        _payload(tmp_path / "culling_results_ws999.json", b"culling"),
        _payload(generated / "local-folders" / "7" / "manifest.json", b"local"),
        _payload(Path(classifier.CACHE_DIR) / "labels" / "encoder" / "label.npy", b"embedding"),
        _payload(hub / "models--jss367--vireo-onnx-models" / "blobs" / "weights", b"weights"),
    ]
    owned_bytes = sum(p.stat().st_size for p in payloads)
    _payload(generated / "unrelated.bin", b"unrelated" * 999)
    _payload(tmp_path / "unrelated-catalog.bin", b"unrelated" * 999)
    _payload(hub / "models--somebody--unrelated" / "blobs" / "weights", b"unrelated" * 999)
    after = client.get('/api/storage').get_json()
    assert after["total"] - before["total"] == owned_bytes
    assert after["hf_cache"]["size"] == len(b"weights")
    # A blob no snapshot points at is listed as an unreferenced, removable entry.
    assert [(m["key"], m["installed"]) for m in after["hf_cache"]["models"]] == [
        ("jss367/vireo-onnx-models#unreferenced", False),
    ]
    assert after["hf_cache"]["unused_size"] == len(b"weights")
    assert any(location["path"] == str(hub) for location in after["locations"])


def test_storage_measures_exact_external_local_copy_without_its_siblings(app_and_db, tmp_path, tmp_path_factory):
    app, db = app_and_db
    folder = db.photos.list_page()[0]["folder_id"]
    copy = tmp_path_factory.mktemp("external-volume") / "managed-copy"
    app.config["THUMB_CACHE_DIR"] = str(tmp_path / "other" / "thumbs")
    db.conn.execute("INSERT INTO local_folders (root_folder_id, state) VALUES (?, 'active')", (folder,))
    db.conn.execute(
        "INSERT INTO local_folder_mappings (root_folder_id, folder_id, source_path, local_path, is_root) "
        "VALUES (?, ?, '/photos/2024', ?, 1)", (folder, folder, str(copy)),
    )
    db.conn.commit()
    client = app.test_client()
    before = client.get('/api/storage').get_json()
    _payload(copy / "original.NEF", b"original copy")
    _payload(copy.parent / "unrelated.NEF", b"not owned" * 100)
    after = client.get('/api/storage').get_json()
    assert after["total"] - before["total"] == len(b"original copy")
    assert any(location["path"] == str(copy) for location in after["locations"])


def test_retention_policy_validation_and_background_application(app_and_db):
    app, _db = app_and_db
    import config as cfg

    client = app.test_client()
    values = {"retained_cache_max_mb": 123, "retained_cache_max_age_days": 45, "grouping_history_max_mb": 17}
    assert client.post('/api/storage/retention', json={**values, "retained_cache_max_mb": -1}).status_code == 400
    response = client.post('/api/storage/retention', json=values)
    assert response.status_code == 200
    job = wait_for_job_via_client(client, response.get_json()["job_id"], wait_for_history=True)
    assert job["status"] == "completed"
    assert all(cfg.load()[key] == value for key, value in values.items())
    data = client.get('/api/storage/retained').get_json()
    assert data["policy"] == values
    assert data["maintenance"]["id"] == response.get_json()["job_id"]


def test_global_maintenance_job_history_has_bounded_retention(app_and_db):
    app, db = app_and_db
    db.conn.executemany(
        "INSERT INTO job_history (id, type, status, started_at, workspace_id) "
        "VALUES (?, 'retained-cache-maintenance', 'completed', '2024-01-01', NULL)",
        [(f"old-global-{n}",) for n in range(105)],
    )
    db.conn.execute("INSERT INTO job_history (id, workspace_id) VALUES ('workspace-owned', ?)", (db.active_workspace_id,))
    db.commit()
    job_id = app._job_runner.start("retained-cache-maintenance", lambda _job: {"ok": True}, workspace_id=None)
    wait_for_job_via_client(app.test_client(), job_id, wait_for_history=True)
    assert db.conn.execute('SELECT COUNT(*) FROM job_history WHERE workspace_id IS NULL').fetchone()[0] == 100
    assert db.job_history.get('workspace-owned')
