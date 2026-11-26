"""Daily catalog backups and restoring the catalog from one."""

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from db import Database
from services import catalog_backups as cb
from services import photo_ingest, workspace_folders
from wait import wait_for_job_via_client


def _hooks(cancelled=lambda: False):
    events = []
    return cb.BackupHooks(cancelled=cancelled, progress=lambda *e: events.append(e)), events


def _catalog(tmp_path, photos=3):
    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        folder_id = workspace_folders.add_folder(db, "/photos/a")
        for i in range(photos):
            photo_ingest.add_photo(db, folder_id=folder_id, filename=f"{i}.jpg", extension=".jpg",
                                   file_size=1, file_mtime=1.0)
        db.commit()
    return db_path


def _add_photos(db_path, count):
    with Database(db_path) as db:
        folder_id = workspace_folders.add_folder(db, "/photos/b")
        for i in range(count):
            photo_ingest.add_photo(db, folder_id=folder_id, filename=f"new{i}.jpg", extension=".jpg",
                                   file_size=1, file_mtime=1.0)
        db.commit()


def _photo_count(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
    finally:
        conn.close()


def _results(tmp_path, workspace_id, text):
    (tmp_path / f"pipeline_results_ws{workspace_id}.json").write_text(text)


def _write_mask_snapshot(media_dir, photo_id, ref, payload=b"\x89PNG\r\n\x1a\n"):
    """Create a file at the content-addressed snapshot path and return it."""
    folder = media_dir / cb.EDIT_MASKS_SUBDIR
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{photo_id}.{ref}.png"
    path.write_bytes(payload)
    return path


def _add_local_recipe(db_path, photo_id, ref):
    """Save a recipe whose ``local.mask.ref`` matches a snapshot file."""
    with Database(db_path) as db:
        db.conn.execute(
            "INSERT OR REPLACE INTO photo_edit_recipes (photo_id, recipe_json) VALUES (?, ?)",
            (photo_id, json.dumps({"local": {"mask": {"ref": ref}}})),
        )
        db.commit()


def test_backup_copies_catalog_and_result_files(tmp_path):
    db_path = _catalog(tmp_path)
    _results(tmp_path, 1, '{"v": "live"}')
    hooks, events = _hooks()

    result = cb.create_backup(db_path, kind="manual", keep=3, hooks=hooks)

    folder = tmp_path / "backups" / result["backup"]
    assert _photo_count(str(folder / cb.BACKUP_DB)) == 3
    assert (folder / "pipeline_results_ws1.json").read_text() == '{"v": "live"}'
    manifest = json.loads((folder / cb.MANIFEST).read_text())
    assert manifest["kind"] == "manual"
    assert manifest["photos"] == 3
    assert manifest["schema_version"] == cb.SCHEMA_VERSION
    assert result["photos"] == 3
    assert "Backed up the catalog" in result["summary"]
    assert any(phase == "Checking the copy" for phase, *_ in events)
    assert not [p for p in os.listdir(tmp_path / "backups") if p.startswith(cb.PARTIAL_PREFIX)]


def test_backup_keeps_the_newest_copies_and_the_one_a_restore_waits_for(tmp_path):
    db_path = _catalog(tmp_path)
    start = datetime(2026, 11, 1, tzinfo=UTC)
    names = [
        cb.create_backup(db_path, kind="daily", keep=5, hooks=_hooks()[0],
                         now=start + timedelta(days=day))["backup"]
        for day in range(3)
    ]
    cb.stage_restore(db_path, names[0], _hooks()[0])

    result = cb.create_backup(db_path, kind="daily", keep=2, hooks=_hooks()[0],
                              now=start + timedelta(days=3))

    remaining = {b["name"] for b in cb.list_backups(db_path)}
    assert result["removed_backups"] == [names[1]]
    assert remaining == {result["backup"], names[2], names[0]}


def test_keep_zero_still_keeps_the_newest_copy(tmp_path):
    db_path = _catalog(tmp_path)
    start = datetime(2026, 11, 1, tzinfo=UTC)
    cb.create_backup(db_path, kind="manual", keep=0, hooks=_hooks()[0], now=start)
    latest = cb.create_backup(db_path, kind="manual", keep=0, hooks=_hooks()[0],
                              now=start + timedelta(hours=1))
    assert [b["name"] for b in cb.list_backups(db_path)] == [latest["backup"]]


def test_backup_refuses_when_the_disk_is_nearly_full(tmp_path, monkeypatch):
    db_path = _catalog(tmp_path)
    monkeypatch.setattr(cb.shutil, "disk_usage", lambda _p: type("U", (), {"free": 10})())
    with pytest.raises(cb.BackupError, match="Not enough free disk space"):
        cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])
    assert cb.list_backups(db_path) == []


def test_cancelled_backup_leaves_no_partial_folder(tmp_path):
    db_path = _catalog(tmp_path, photos=200)
    hooks, _events = _hooks(cancelled=lambda: True)
    with pytest.raises(cb.BackupError, match="cancelled"):
        cb.create_backup(db_path, kind="daily", keep=3, hooks=hooks)
    assert os.listdir(tmp_path / "backups") == []


def test_daily_backup_is_due_after_the_interval(tmp_path):
    db_path = _catalog(tmp_path)
    made = datetime(2026, 11, 1, 8, tzinfo=UTC)
    assert cb.is_due(db_path, made)
    cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0], now=made)
    assert not cb.is_due(db_path, made + timedelta(hours=22))
    assert cb.is_due(db_path, made + cb.DAILY_INTERVAL)


def test_restore_replaces_catalog_and_keeps_the_previous_one(tmp_path):
    db_path = _catalog(tmp_path, photos=3)
    _results(tmp_path, 1, "backup-era")
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    _add_photos(db_path, 2)
    _results(tmp_path, 1, "newer")
    _results(tmp_path, 2, "newer workspace")

    cb.stage_restore(db_path, backup, _hooks()[0])
    assert cb.pending_restore(db_path)["backup"] == backup
    assert _photo_count(db_path) == 5  # nothing changes until the next launch

    outcome = cb.apply_pending_restore(db_path)

    assert outcome["ok"], outcome
    assert _photo_count(db_path) == 3
    assert (tmp_path / "pipeline_results_ws1.json").read_text() == "backup-era"
    assert not (tmp_path / "pipeline_results_ws2.json").exists()
    before = tmp_path / "backups" / outcome["before_restore"]
    assert _photo_count(str(before / cb.BACKUP_DB)) == 5
    assert (before / "pipeline_results_ws2.json").read_text() == "newer workspace"
    assert json.loads((before / cb.MANIFEST).read_text())["kind"] == cb.BEFORE_RESTORE
    assert cb.pending_restore(db_path) is None
    assert not (tmp_path / "backups" / cb.STAGED_DB).exists()
    assert cb.last_restore(db_path)["ok"] is True
    assert cb.apply_pending_restore(db_path) is None

    # The set-aside catalog is itself restorable, which undoes the restore.
    cb.stage_restore(db_path, outcome["before_restore"], _hooks()[0])
    assert cb.apply_pending_restore(db_path)["ok"]
    assert _photo_count(db_path) == 5
    assert (tmp_path / "pipeline_results_ws2.json").read_text() == "newer workspace"


def test_restored_catalog_opens(tmp_path):
    db_path = _catalog(tmp_path)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    cb.stage_restore(db_path, backup, _hooks()[0])
    cb.apply_pending_restore(db_path)
    with Database(db_path) as db:
        assert db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == 3


def test_restore_folds_the_live_write_ahead_log_into_the_set_aside_copy(tmp_path):
    db_path = _catalog(tmp_path, photos=1)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    cb.stage_restore(db_path, backup, _hooks()[0])
    # A write still in the WAL when Vireo last quit.
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("INSERT INTO photos(folder_id, filename, extension, file_size, file_mtime) "
                 "SELECT folder_id, 'wal.jpg', '.jpg', 1, 1 FROM photos LIMIT 1")
    conn.commit()
    assert os.path.getsize(db_path + "-wal") > 0
    conn.close()

    outcome = cb.apply_pending_restore(db_path)

    before = tmp_path / "backups" / outcome["before_restore"] / cb.BACKUP_DB
    assert _photo_count(str(before)) == 2
    assert _photo_count(db_path) == 1


def test_restore_resumes_after_an_interrupted_launch(tmp_path, monkeypatch):
    db_path = _catalog(tmp_path, photos=2)
    _results(tmp_path, 1, "backup-era")
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    _add_photos(db_path, 1)
    _results(tmp_path, 1, "newer")
    cb.stage_restore(db_path, backup, _hooks()[0])

    def crash(*_args):
        raise KeyboardInterrupt("power cut")

    monkeypatch.setattr(cb, "_put_backup_in_place", crash)
    with pytest.raises(KeyboardInterrupt):
        cb.apply_pending_restore(db_path)
    assert not os.path.exists(db_path)
    assert cb.pending_restore(db_path)["phase"] == "moved_live"
    monkeypatch.undo()

    outcome = cb.apply_pending_restore(db_path)

    assert outcome["ok"]
    assert _photo_count(db_path) == 2
    before = tmp_path / "backups" / outcome["before_restore"]
    assert _photo_count(str(before / cb.BACKUP_DB)) == 3
    assert (before / "pipeline_results_ws1.json").read_text() == "newer"
    assert (tmp_path / "pipeline_results_ws1.json").read_text() == "backup-era"


def test_failed_swap_puts_the_live_catalog_back(tmp_path, monkeypatch):
    db_path = _catalog(tmp_path, photos=2)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    _add_photos(db_path, 1)
    _results(tmp_path, 3, "live")
    cb.stage_restore(db_path, backup, _hooks()[0])
    real_replace = cb.replace_file

    def failing_replace(src, dst):
        if src.endswith(cb.STAGED_DB):
            raise PermissionError("locked")
        real_replace(src, dst)

    monkeypatch.setattr(cb, "replace_file", failing_replace)
    outcome = cb.apply_pending_restore(db_path)

    assert not outcome["ok"]
    assert "locked" in outcome["error"]
    assert _photo_count(db_path) == 3
    assert (tmp_path / "pipeline_results_ws3.json").read_text() == "live"
    assert [b["kind"] for b in cb.list_backups(db_path)] == ["daily"]
    assert cb.pending_restore(db_path) is None


def test_missing_staged_copy_changes_nothing(tmp_path):
    db_path = _catalog(tmp_path, photos=2)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    cb.stage_restore(db_path, backup, _hooks()[0])
    os.remove(tmp_path / "backups" / cb.STAGED_DB)

    outcome = cb.apply_pending_restore(db_path)

    assert not outcome["ok"]
    assert "missing" in outcome["error"]
    assert _photo_count(db_path) == 2
    assert cb.last_restore(db_path)["ok"] is False


def test_backup_from_a_newer_vireo_is_not_restorable(tmp_path):
    db_path = _catalog(tmp_path)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    manifest_path = tmp_path / "backups" / backup / cb.MANIFEST
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = cb.SCHEMA_VERSION + 1
    manifest_path.write_text(json.dumps(manifest))

    [listed] = cb.list_backups(db_path)
    assert not listed["restorable"]
    assert "newer version" in listed["not_restorable_reason"]
    with pytest.raises(cb.BackupError, match="newer version"):
        cb.stage_restore(db_path, backup, _hooks()[0])


def test_cancel_restore_drops_the_staged_copy(tmp_path):
    db_path = _catalog(tmp_path)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0])["backup"]
    cb.stage_restore(db_path, backup, _hooks()[0])
    assert cb.cancel_restore(db_path) is True
    assert cb.pending_restore(db_path) is None
    assert not (tmp_path / "backups" / cb.STAGED_DB).exists()
    assert cb.apply_pending_restore(db_path) is None
    assert cb.cancel_restore(db_path) is False


def test_backup_includes_referenced_edit_mask_snapshots(tmp_path):
    db_path = _catalog(tmp_path)
    referenced = "0123456789ab"
    orphan = "ba9876543210"
    _write_mask_snapshot(tmp_path, 1, referenced, payload=b"kept")
    _write_mask_snapshot(tmp_path, 2, orphan, payload=b"orphan")
    _add_local_recipe(db_path, 1, referenced)

    result = cb.create_backup(db_path, kind="manual", keep=3, hooks=_hooks()[0],
                              media_dir=str(tmp_path))

    folder = tmp_path / "backups" / result["backup"]
    backed_up = folder / cb.EDIT_MASKS_SUBDIR / f"1.{referenced}.png"
    assert backed_up.read_bytes() == b"kept"
    assert not (folder / cb.EDIT_MASKS_SUBDIR / f"2.{orphan}.png").exists()
    assert json.loads((folder / cb.MANIFEST).read_text())["edit_masks"] == 1


def test_backup_without_edit_masks_folder(tmp_path):
    db_path = _catalog(tmp_path)

    result = cb.create_backup(db_path, kind="manual", keep=3, hooks=_hooks()[0],
                              media_dir=str(tmp_path))

    folder = tmp_path / "backups" / result["backup"]
    assert json.loads((folder / cb.MANIFEST).read_text())["edit_masks"] == 0
    assert not (folder / cb.EDIT_MASKS_SUBDIR).exists()


def test_restore_rewrites_gc_deleted_mask_snapshots(tmp_path):
    db_path = _catalog(tmp_path)
    ref = "0123456789ab"
    snapshot = _write_mask_snapshot(tmp_path, 1, ref, payload=b"frozen")
    _add_local_recipe(db_path, 1, ref)

    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0],
                              media_dir=str(tmp_path))["backup"]
    snapshot.unlink()  # stand-in for local_masks.gc_edit_masks reclaiming it
    cb.stage_restore(db_path, backup, _hooks()[0])

    outcome = cb.apply_pending_restore(db_path, media_dir=str(tmp_path))

    assert outcome["ok"], outcome
    assert snapshot.read_bytes() == b"frozen"


def test_restore_preserves_pre_restore_masks_for_undo(tmp_path):
    db_path = _catalog(tmp_path)
    backup_ref, live_ref = "aaaa11112222", "bbbb33334444"
    _write_mask_snapshot(tmp_path, 1, backup_ref, payload=b"old")
    _add_local_recipe(db_path, 1, backup_ref)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0],
                              media_dir=str(tmp_path))["backup"]

    _write_mask_snapshot(tmp_path, 1, live_ref, payload=b"new")
    _add_local_recipe(db_path, 1, live_ref)
    cb.stage_restore(db_path, backup, _hooks()[0])

    outcome = cb.apply_pending_restore(db_path, media_dir=str(tmp_path))

    before_dir = tmp_path / "backups" / outcome["before_restore"] / cb.EDIT_MASKS_SUBDIR
    assert (before_dir / f"1.{live_ref}.png").read_bytes() == b"new"
    manifest = json.loads(
        (tmp_path / "backups" / outcome["before_restore"] / cb.MANIFEST).read_text()
    )
    assert manifest["edit_masks"] == 1

    # And undoing the restore by restoring the before-restore copy puts the
    # live-era snapshot back if a later GC had removed it.
    (tmp_path / cb.EDIT_MASKS_SUBDIR / f"1.{live_ref}.png").unlink()
    cb.stage_restore(db_path, outcome["before_restore"], _hooks()[0])
    assert cb.apply_pending_restore(db_path, media_dir=str(tmp_path))["ok"]
    assert (tmp_path / cb.EDIT_MASKS_SUBDIR / f"1.{live_ref}.png").read_bytes() == b"new"


def test_restore_keeps_existing_live_snapshots(tmp_path):
    db_path = _catalog(tmp_path)
    ref = "0123456789ab"
    _write_mask_snapshot(tmp_path, 1, ref, payload=b"snapshot")
    _add_local_recipe(db_path, 1, ref)
    backup = cb.create_backup(db_path, kind="daily", keep=3, hooks=_hooks()[0],
                              media_dir=str(tmp_path))["backup"]
    cb.stage_restore(db_path, backup, _hooks()[0])

    # A caller-local tampering with the live snapshot proves the restore
    # does not overwrite a snapshot that's already on disk.
    (tmp_path / cb.EDIT_MASKS_SUBDIR / f"1.{ref}.png").write_bytes(b"locally edited")
    cb.apply_pending_restore(db_path, media_dir=str(tmp_path))

    assert (tmp_path / cb.EDIT_MASKS_SUBDIR / f"1.{ref}.png").read_bytes() == b"locally edited"


def test_api_backs_up_and_prepares_a_restore(app_and_db):
    app, _db = app_and_db
    client = app.test_client()

    status = client.get("/api/catalog-backups").get_json()
    assert status["backups"] == []
    assert status["keep"] == 3

    job_id = client.post("/api/catalog-backups").get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    [backup] = client.get("/api/catalog-backups").get_json()["backups"]
    assert backup["kind"] == "manual"
    assert backup["photos"] == 3

    assert client.post("/api/catalog-backups/nope/restore").status_code == 404
    job_id = client.post(f"/api/catalog-backups/{backup['name']}/restore").get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    status = client.get("/api/catalog-backups").get_json()
    assert status["pending_restore"]["backup"] == backup["name"]

    assert client.delete("/api/catalog-backups/pending-restore").get_json() == {"cancelled": True}
    assert client.get("/api/catalog-backups").get_json()["pending_restore"] is None
